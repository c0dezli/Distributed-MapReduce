/*
 * Implementation file for simple MapReduce framework.  Fill in the functions
 * and definitions in this file to complete the assignment.
 *
 * Place all of your implementation code in this file.  You are encouraged to
 * create helper functions wherever it is necessary or will make your code
 * clearer.  For these functions, you should follow the practice of declaring
 * them "static" and not including them in the header file (which should only be
 * used for the *public-facing* API.
 */

/* Header includes */
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <fcntl.h>
#include "mapreduce.h"
#include <netinet/in.h>
#include <arpa/inet.h>

/* Size of shared memory buffers */
#define MR_BUFFER_SIZE 1024

// The args for map/reduce function
struct args_helper {
 struct map_reduce *mr;
 int infd, outfd, nmaps, id;
 map_fn map;
 reduce_fn reduce;
};

/* Helper function that can be passed to the pthread_create to call the map_fn
 */
static void *map_wrapper(void* map_args) {
  // Reconstruct the Arguments
  struct args_helper *args = (struct args_helper *) map_args;
  // Call the map function and save the return value
  args->mr->mapfn_status[args->id] =
      args->map(args->mr, args->infd, args->id, args->nmaps);
  // Send a signal to mr_consume after the function returns
  // pthread_cond_signal(&args->mr->not_empty[args->id]);
  printf("Client %d: Created Map thread\n", args->id);
  return NULL;
}

/* Helper function that can be passed to the pthread_create to call the reduce_fn
 */
static void *reduce_wrapper(void* reduce_args) {
  // Reconstruct the Arguments
  struct args_helper *args = (struct args_helper *) reduce_args;

  //http://www.binarytides.com/multiple-socket-connections-fdset-select-linux/
  // Connect all the clients
  for(int i=0; i<args->mr->nmaps; i++){
    socklen_t addrlen = sizeof(args->mr->client_addr[i]);
    args->mr->client_sockfd[i] =
      accept(args->mr->server_sockfd, (struct sockaddr *)&args->mr->client_addr[i], &addrlen);

    if (args->mr->client_sockfd[i] < 0) {
      printf("Server: Cannot build connection for client %d.\n", i);
      perror("Error message");
      return NULL;
    }

  }
  printf("Server: All clients connected!\n");

  // Call the reduce function and save the return value
  args->mr->reducefn_status =
    args->reduce(args->mr, args->outfd, args->nmaps);
  return NULL;
}


void receive_bytes_check(int receive_bytes, int id){
   if (receive_bytes == 0) {
       printf("Server: client %d send nothing\n", id);
   }
   if (receive_bytes < 0) {
       printf("Server: ERROR reading key from socket, client %d.\n", id);
   }
}

/*
Refs:
http://stackoverflow.com/questions/29350073/invalid-write-of-size-8-after-a-malloc
*/
struct map_reduce*
mr_create(map_fn map, reduce_fn reduce, int nmaps) {
   struct map_reduce *mr = malloc (sizeof(struct map_reduce));

   if(mr == 0) {  // Check Success
     free(mr);
     return NULL;
   }

   // Check if it's server or client
   if(map == NULL){
     //client
     mr->client = false;
     mr->server = true;
   }

   if (reduce == NULL){
     //server
     mr->client = true;
     mr->server = false;
   }

   if(reduce == NULL && map == NULL)
     return NULL;

   // Save the Parameters
   mr->map             = map;
   mr->reduce          = reduce;
   mr->nmaps       = nmaps;

   // File Descriptors
   mr->outfd           = -1;
   mr->infd            = malloc(nmaps * sizeof(int));

   // Sockets
   mr->server_sockfd   = -1;
   mr->client_sockfd   = malloc(nmaps * sizeof(int));
   for(int i=0; i<nmaps; i++)
      mr->client_sockfd[i] = 0;

   mr->client_addr    = malloc(nmaps * sizeof(struct sockaddr_in));

   // Threads
   mr->map_threads     = malloc(nmaps * sizeof(pthread_t));
   mr->mapfn_status    = malloc(nmaps * sizeof(int));
   for(int i=0; i<nmaps; i++)
      mr->mapfn_status[i] = -1;
   mr->reducefn_status = -1;

   // Arguments of Funtion Wappers
   mr->args            = malloc((nmaps + 1) * sizeof(struct args_helper));

   // Lock & Conditional Variables
   mr->_lock           = malloc(nmaps * sizeof(pthread_mutex_t));
   mr->not_full        = malloc(nmaps * sizeof(pthread_cond_t));
   mr->not_empty       = malloc(nmaps * sizeof(pthread_cond_t));

   for (int i=0; i<nmaps; i++) {  // Init
       pthread_mutex_init(&mr->_lock[i], NULL);
       pthread_cond_init(&mr->not_full[i], NULL);
       pthread_cond_init(&mr->not_empty[i], NULL);
   }

   // Init the Buffer List
   mr->buffer = malloc(nmaps * sizeof(char*));
   mr->size   = malloc(nmaps * sizeof(int));

   for(int i = 0; i < nmaps; i++){
     mr->buffer[i] = malloc(MR_BUFFER_SIZE * sizeof(char));
     mr->size[i] = 0;
   }
	 return mr;
}

/* Destroys and cleans up an existing instance of the MapReduce framework */
void
mr_destroy(struct map_reduce *mr) {
  for(int i=0; i<mr->nmaps; i++){
    free(mr->buffer[i]);
  }
  free(mr->buffer);
  free(mr->infd);
  free(mr->client_addr);
  free(mr->client_sockfd);
  free(mr->map_threads);
  free(mr->mapfn_status);
  free(mr->not_full);
  free(mr->not_empty);
  free(mr->_lock);
  free(mr->size);
  free(mr->args);
  free(mr);
}

/* Begins a multithreaded MapReduce operation */
int
mr_start(struct map_reduce *mr, const char *path, const char *ip, uint16_t port)
{
  if(mr->server)
  {
  	// Open the output file
    mr->outfd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 644);

  	// mr->outfd = socket(AF_INET, SOCK_STREAM, 0);
  	if (mr->outfd < 0) {
	     close(mr->outfd);
	     perror("Server: Cannot open ouput file.\n");
	     return -1;
	  }

    // Open the socket
    mr->server_sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (mr->server_sockfd < 0) {
      close(mr->outfd);
      close(mr->server_sockfd);
      perror("Server: Cannot open socket.\n");
      return -1;
    }

    // Setup the address info
    mr->server_addr.sin_family = AF_INET;
    mr->server_addr.sin_port = htons(port);
    mr->server_addr.sin_addr.s_addr = INADDR_ANY;

    // Bind the socket and address
    if (bind(mr->server_sockfd, (struct sockaddr *) &mr->server_addr, sizeof(struct sockaddr)) == -1) {
      perror("Server: Cannot bind socket.\n");
      return -1;
    }

    // Start Listen
    if (listen(mr->server_sockfd, mr->nmaps) == -1 ) {
      perror("Server: Cannot start socket listen.\n");
      return -1;
    }
    printf("Server: Start listening for connections.\n");


    // Construct the reduce arguments
    struct args_helper *reduce_args;
  	reduce_args         = &(mr->args[mr->nmaps]);
  	reduce_args->mr     = mr;
  	reduce_args->reduce = mr->reduce;
  	reduce_args->map    = mr->map;
  	reduce_args->outfd  = mr->outfd;
  	reduce_args->nmaps  = mr->nmaps;

  	// Create reduce thread
	  if (pthread_create(&mr->reduce_thread, NULL, &reduce_wrapper, (void *)reduce_args) != 0) {
	    perror("Server: Failed to create reduce thread");
	    return -1;
    }
  	// Success
  	return 0;
  }

  if(mr->client)
  {
  	// Create n threads for map function (n = nmaps)
  	for(int i=0; i<(mr->nmaps); i++) {

    	//Assign different socketfd to every map thread
      mr->infd[i] = open(path, O_WRONLY | O_CREAT | O_TRUNC, 644);
    	if (mr->infd[i] < 0) {
    	  close(mr->infd[i]);
    	  perror("Client: Cannot open input file");
    	  return -1;
    	}

      // Create socket
      mr->client_sockfd[i] = socket(AF_INET, SOCK_STREAM, 0);
      if (mr->client_sockfd[i] < 0){
        close(mr->infd[i]);
        close(mr->client_sockfd[i]);
        perror("Client: Cannot open socket");
      }

      // Setup the address info
      mr->server_addr.sin_family = AF_INET;
      mr->server_addr.sin_port = htons(port);
      if(inet_aton(ip, &mr->server_addr.sin_addr) == 0) {
        perror("Client: Cannot set ip");
        return -1;
      }

      // Connect to server
      //http://www.cse.psu.edu/~djp284/cmpsc311-s15/slides/25-networking.pdf
      if (connect(mr->client_sockfd[i], (struct sockaddr *)&mr->server_addr, sizeof(mr->server_addr)) < 0){
        perror("Client: ERROR connecting to server");
        return -1;
      }

      printf("Client %d: Connected with server, socketfd is %d.\n", i, mr->client_sockfd[i]);

      // Construct the map arguments
      struct args_helper *map_args;
      map_args         = &(mr->args[i]);
      map_args->mr     = mr;
      map_args->map    = mr->map;
      map_args->reduce = mr->reduce;
      map_args->infd   = mr->infd[i];
      map_args->id     = i;
      map_args->nmaps  = mr->nmaps;

      // Create map threads
      if(pthread_create(&mr->map_threads[i], NULL, &map_wrapper, (void *)map_args) != 0) {
  	     perror("Client: Failed to create map thread");
  	     return -1;
      }
    }
    // Success
    return 0;
  }
  // No client and no server
  return -1;
}

/* Blocks until the entire MapReduce operation is complete */
int
mr_finish(struct map_reduce *mr) {
  if(mr->server){
    // Wait reduce fn finish
    if(pthread_join(mr->reduce_thread, NULL)) {
      perror("Server: Failed to wait a map thead end");
      return -1;
    }

    // Close server socket and file
    if(close(mr->server_sockfd) != 0){
      perror("Server: Failed to close socket connection");
      return -1;
    }
    if(close(mr->outfd) != 0){
      perror("Server: Failed to close file");
      return -1;
    }
    for(int i=0; i<(mr->nmaps); i++) {
      if(close(mr->client_sockfd[i]) != 0){
        perror("Server: Failed to close client socket connection");
        return -1;
      }
    }

    // Check status
    if(mr->reducefn_status != 0)
      return -1;
  }
  else if (mr->client){
    // Wait all map function finish
    for(int i=0; i<(mr->nmaps); i++) {
      if(pthread_join(mr->map_threads[i], NULL)) {
        perror("Client: Failed to wait a map thead end");
        return -1;
      }
    }

    // Close socket and file
    for(int i=0; i<(mr->nmaps); i++) {
      if(close(mr->client_sockfd[i]) != 0){
        perror("Client: Failed to close socket connection");
        return -1;
      }
      if(close(mr->infd[i]) != 0){
        perror("Client: Failed to close infd");
        return -1;
      }
    }

    // Check status
    for(int i=0; i<(mr->nmaps); i++) {
      if (mr->mapfn_status[i] != 0)
        return -1;
      }
  }
  // Pass all the check, then success
  return 0;
}

/* Called by the Map function each time it produces a key-value pair */
int
mr_produce(struct map_reduce *mr, int id, const struct kvpair *kv) {
  // Lock
  // pthread_mutex_lock(&mr->_lock[id]);
  // Get the kv_pair size

   printf("Client: Trying to send value to server.\n");

   int kv_size = kv->keysz + kv->valuesz + 8;
   int value;


   recv(mr->client_sockfd[id], &value, sizeof(value), 0);
   printf("Client %d, get value %d from server\n", id,  ntohl(value));


   value = htonl(1);
   if(send(mr->client_sockfd[id], &value, sizeof(value), 0) < 0) {
      perror("Client: ERROR sending map function status.");
      return -1;
    } else {
      printf("Client %d, Send value %d to server\n", id,  ntohl(value));
    }

   // Send the map function status
  //  value = htonl(mr->mapfn_status[id]);
  //  if(send(mr->client_sockfd[id], &value, sizeof(value), 0) < 0) {
  //    perror("Client: ERROR sending map function status.");
  //    return -1;
  //  }
   //
  //  value = htonl(kv_size);
  //  if(send(mr->client_sockfd[id], &value, sizeof(value), 0) < 0) {
  //    perror("Client: ERROR sending kv pair size");
  //    return -1;
  //  }
   //
  //  if(send(mr->client_sockfd[id], kv, kv_size, 0 ) < 0){
  //     perror("Client: ERROR sending kv pair");
  //     return -1;
  //   }


  //Send the signal
  // pthread_cond_signal (&mr->not_empty[id]);
  // // Unlock
  // pthread_mutex_unlock(&mr->_lock[id]);
  // Success
 return 0;
}

/* Called by the Reduce function to consume a key-value pair */
int
mr_consume(struct map_reduce *mr, int id, struct kvpair *kv) {
  //char * buffer[50];
  int fn_result = -1,
      receive_bytes = -1,
      kv_size = -1;
  uint32_t value;
  // Block until some value is in buffer
  while(true){

    value = htonl(1);
    send(mr->client_sockfd[id], &value, sizeof(value), 0);
    printf("Server: Send value %d to client %d\n", ntohl(value), id );

    // Test
    receive_bytes = recv(mr->client_sockfd[id], &value, sizeof(value), 0);
    while(receive_bytes != sizeof(value)) {
      receive_bytes = recv(mr->client_sockfd[id], &value, sizeof(value), 0);
    }
    printf("Server: Get a value %d\n", ntohl(value));

    // Get Funtion Return Value
    // receive_bytes = recv(mr->client_sockfd[id], &fn_result, 4, 0);
    // if(receive_bytes != 4) {
    //   receive_bytes_check(receive_bytes, id);
    //   return -1;
    // }
    // else if(htonl(fn_result) == 0) return 0;
    //
    // // Get the kv pair size
    // else if(i == 1){
    //   receive_bytes = recv(mr->client_sockfd[id], &kv_size, 4, 0);
    //   if(receive_bytes != 4) {
    //     receive_bytes_check(receive_bytes, id);
    //     return -1;
    //   }
    // }
    //
    // // Get the kv pair
    // else {
    //   receive_bytes = recv(mr->client_sockfd[id], kv, kv_size, 0);
    //   if(receive_bytes != kv_size) {
    //     receive_bytes_check(receive_bytes, id);
    //     return -1;
    //   }
    // }
  }
  return 0;
}
