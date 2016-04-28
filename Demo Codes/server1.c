/* A simple server in the internet domain using TCP
   The port number is passed as an argument
   The server reads in (word, count) pairs sent by a single client.
*/
#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <unistd.h>
#include <strings.h>
#include <stdlib.h>

int main(int argc, char *argv[])
{
     int sockfd, newsockfd, portno;
     socklen_t  clilen;
     char buffer[20];
     struct sockaddr_in serv_addr, cli_addr;
     struct hostent *server;
     int n, tmp, value;

     if (argc < 2) {
         fprintf(stderr,"ERROR, no port provided\n");
         exit(1);
     }
     sockfd = socket(AF_INET, SOCK_STREAM, 0);
     if (sockfd < 0)
        perror("ERROR opening socket");

     bzero((char *) &serv_addr, sizeof(serv_addr));

     portno = atoi(argv[1]);

     server = gethostbyname ("localhost");
     serv_addr.sin_addr.s_addr = INADDR_ANY;
     serv_addr.sin_family = AF_INET;

     bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);

     serv_addr.sin_port = htons(portno);

     if (bind(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0)
        perror("ERROR on binding");

     listen(sockfd,5);
     clilen = sizeof(cli_addr);
     newsockfd = accept(sockfd, (struct sockaddr *) &cli_addr, &clilen);
     if (newsockfd < 0)
          perror("ERROR on accept");

     while (1) {
        bzero (buffer,20);

        n = recv (newsockfd,buffer, sizeof (buffer), 0);

        if (n == 0) {
		        printf ("Server: client closed connection\n");
		        break;
	      }

        if (n < 0) perror("ERROR reading key from socket");

	      n = recv (newsockfd, &tmp, sizeof (tmp), 0);
	      value = htonl (tmp);

        if (n == 0) {
		        printf("Server: client closed connection\n");
		        break;
	      }

        if (n < 0) perror ("ERROR reading value from socket");

        printf("Server received: key=%s, value=%d\n",buffer, value);
     }
     close (newsockfd);
     return 0;
}
