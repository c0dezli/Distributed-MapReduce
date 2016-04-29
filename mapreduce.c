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
  return NULL;
}

/* Helper function that can be passed to the pthread_create to call the reduce_fn
 */
static void *reduce_wrapper(void* reduce_args) {
  // Reconstruct the Arguments
  struct args_helper *args = (struct args_helper *) reduce_args;

  //http://www.binarytides.com/multiple-socket-connections-fdset-select-linux/
  // Connect all the clients
  socklen_t addrlen = sizeof(struct sockaddr_in);

  for(int i=0; i<args->mr->nmaps; i++){
    args->mr->client_sockfd[i] =
      accept(args->mr->server_sockfd, (struct sockaddr *)&args->mr->client_addr[i], &addrlen);

    if (args->mr->client_sockfd[i] < 0) {
      printf("Server: Cannot build connection for client %d.\n", i);
      perror("Error message");
      close(args->mr->outfd);
      close(args->mr->client_sockfd[i]);
      args->mr->reducefn_status = -1;
      return NULL;
    }
  }
  printf("Server: All clients connected!\n");

  // Call the reduce function and save the return value
  args->mr->reducefn_status =
    args->reduce(args->mr, args->outfd, args->nmaps);
  return NULL;
}

/*
Refs:
http://stackoverflow.com/questions/29350073/invalid-write-of-size-8-after-a-malloc
*/
struct map_reduce*
mr_create(map_fn map, reduce_fn reduce, int nmaps) {
   struct map_reduce *mr = malloc (sizeof(struct map_reduce));

   if(mr == NULL) {  // Check Success
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
   mr->nmaps           = nmaps;

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
  if(mr->client)
  {
    // Setup the address info
    mr->server_addr.sin_family = AF_INET;
    mr->server_addr.sin_port = htons(port);
    inet_aton(ip,&mr->server_addr.sin_addr);

  	// Create n threads for map function (n = nmaps)
  	for(int i=0; i<(mr->nmaps); i++) {

    	//Assign different socketfd to every map thread
      mr->infd[i] = open(path, O_RDONLY, 644);
    	if (mr->infd[i] < 0) {
    	  close(mr->infd[i]);
    	  perror("Client: Cannot open input file");
    	  return -1;
    	}

      // Create socket
      mr->client_sockfd[i] = socket(AF_INET, SOCK_STREAM, 0);
      if (mr->client_sockfd[i] == -1){
        close(mr->infd[i]);
        close(mr->client_sockfd[i]);
        perror("Client: Cannot open socket");
        return -1;
      }

      // Connect to server
      //http://www.cse.psu.edu/~djp284/cmpsc311-s15/slides/25-networking.pdf
      if(connect(mr->client_sockfd[i], (struct sockaddr *)&mr->server_addr, sizeof(mr->server_addr)) == -1){
        perror("Client: ERROR connecting to server");
        return -1;
      } else
        printf("Client %d: Connected with server, socketfd is %d.\n", i, mr->client_sockfd[i]);

      // Construct the map arguments
      struct args_helper *map_args;
      map_args         = &(mr->args[i]);
      map_args->mr     = mr;
      map_args->map    = mr->map;
      map_args->infd   = mr->infd[i];
      map_args->id     = i;
      map_args->nmaps  = mr->nmaps;

      // Create map threads
      if(pthread_create(&mr->map_threads[i], NULL, map_wrapper, (void *)map_args) != 0) {
  	     perror("Client: Failed to create map thread");
  	     return -1;
      }
    }
    // Success
    return 0;
  }
  if(mr->server)
  {
    // Setup the address info
    mr->server_addr.sin_family = AF_INET;
    mr->server_addr.sin_port = htons(port);
    mr->server_addr.sin_addr.s_addr	= INADDR_ANY;

    // Open the output file
    mr->outfd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 644);

    // mr->outfd = socket(AF_INET, SOCK_STREAM, 0);
    if (mr->outfd < 0) {
       perror("Server: Cannot open ouput file.\n");
       return -1;
    }

    // Open the socket
    mr->server_sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (mr->server_sockfd < 0) {
      perror("Server: Cannot open socket.\n");
      return -1;
    }

    // Set server socket to allow multiple connections
    if (setsockopt(mr->server_sockfd, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int)) ==-1) {
      perror("Server: Cannot set sockopt.\n");
      return -1;
    }

    // Bind the socket and address
    if (bind(mr->server_sockfd, (struct sockaddr *) &mr->server_addr, sizeof(mr->server_addr)) == -1) {
      perror("Server: Cannot bind socket.\n");
      return -1;
    }

    // Start Listen
    if (listen(mr->server_sockfd, mr->nmaps) == -1) {
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
    if (pthread_create(&mr->reduce_thread, NULL, reduce_wrapper, (void *)reduce_args) != 0) {
      perror("Server: Failed to create reduce thread");
      return -1;
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
  if(mr->client){
    printf("Client: I'm going to suiside now!\n");
    // Wait all map function finish
    for(int i=0; i<(mr->nmaps); i++) {
      if(pthread_join(mr->map_threads[i], NULL)) {
        perror("Client: Failed to wait a map thead end");
        return -1;
      }
    }
    printf("Client: All threads killed.\n");

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
    printf("Client: All sockets and file closed.\n");

    // Check status
    for(int i=0; i<(mr->nmaps); i++) {
      if (mr->mapfn_status[i] != 0)
        return -1;
      }
  }
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

    // Close client socket
    for(int i=0; i<(mr->nmaps); i++) {
      if(close(mr->client_sockfd[i]) != 0){
        perror("Client: Failed to close socket connection");
        return -1;
      }
    }

    // Check status
    if(mr->reducefn_status != 0)
      return -1;
  }

  // Pass all the test, then success
  return 0;
}

/* Called by the Map function each time it produces a key-value pair */
int
mr_produce(struct map_reduce *mr, int id, const struct kvpair *kv) {
  if(kv==NULL) return -1;
  uint32_t value;

  value = htonl(kv->keysz);
  if(send(mr->client_sockfd[id], &value, sizeof(uint32_t), 0) == -1) {
    perror("Client: Fail to send keysz");
    return -1;
  }
  if(send(mr->client_sockfd[id], kv->key, kv->keysz, 0) == -1) {
    perror("Client: Fail to send key");
    return -1;
  }

  value = htonl(kv->valuesz);
  if(send(mr->client_sockfd[id], &value, sizeof(uint32_t), 0) == -1) {
    perror("Client: Fail to send valuesz");
    return -1;
  }
  if(send(mr->client_sockfd[id], kv->value, kv->valuesz, 0) == -1) {
    perror("Client: Fail to send value");
    return -1;
  }
  //Success
  //close(mr->client_sockfd[id]);
  return 0;
}

/* Called by the Reduce function to consume a key-value pair */
int
mr_consume(struct map_reduce *mr, int id, struct kvpair *kv) {
  // Lock
  // pthread_mutex_lock(&mr->_lock[id]);
  //char * buffer[50];
  int fn_result = -1,
      receive_bytes = -1,
      kv_size = -1;
  uint32_t value;

  //receive_bytes = recv(mr->client_sockfd[id], &fn_result, sizeof(fn_result), 0);
  receive_bytes = recv(mr->client_sockfd[id], &value, sizeof(uint32_t), 0);
  kv->keysz = ntohl(value);

  kv->key = malloc(sizeof(kv->keysz));

  receive_bytes = recv(mr->client_sockfd[id], &value, sizeof(uint32_t), 0);
  kv->valuesz = ntohl(value);

  kv->value = malloc(sizeof(kv->valuesz));

  return 0;
}
