#include <pthread.h>
#include <cstdlib>
#include <map>
#include <cstdint>
#include "lib.h"
#include "utils.h"
#include "protocol.h"
#include <poll.h>
#include <cassert>
#include <sys/timerfd.h>
#include <string.h>
#include <unistd.h>

using namespace std;

std::map<int, struct connection *> cons;

struct pollfd data_fds[MAX_CONNECTIONS];
/* Used for timers per connection */
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;

int main_sockfd = -1;
int server_recv_window = 0;

int recv_data(int conn_id, char *buffer, int len)
{
    int size = 0;

    pthread_mutex_lock(&cons[conn_id]->con_lock);
    
    /* We will write code here as to not have sync problems with recv_handler */
    while(cons[conn_id]->recv_buf.empty()) {
        pthread_mutex_unlock (&cons[conn_id]->con_lock);
        usleep(2000);
        pthread_mutex_lock(&cons[conn_id]->con_lock);
    }
    if (len <= (int)cons[conn_id]->recv_buf.size()) {
        size = len;
    } else {
        size = (int)cons[conn_id]->recv_buf.size();
    }
    memcpy(buffer, cons[conn_id]->recv_buf.data(), size);
    cons[conn_id]->recv_buf.erase(cons[conn_id]->recv_buf.begin(), cons[conn_id]->recv_buf.begin() + size);
    pthread_mutex_unlock(&cons[conn_id]->con_lock);

    return size;
}

void *receiver_handler(void *arg)
{

    char segment[MAX_SEGMENT_SIZE];
    int res;
    DEBUG_PRINT("Starting recviver handler\n");

    while (1) {

        int conn_id = -1;
        do {
            res = recv_message_or_timeout(segment, MAX_SEGMENT_SIZE, &conn_id);
        } while(res == -14);

        pthread_mutex_lock(&cons[conn_id]->con_lock);

        /* Handle segment received from the sender. We use this between locks
        as to not have synchronization issues with the recv_data calls which are
        on the main thread */
        poli_tcp_data_hdr *data = (poli_tcp_data_hdr *) segment;
        if (data->protocol_id == POLI_PROTOCOL_ID && data->type == DATA_TYPE) {
            uint16_t seq_num = data->seq_num;
            if (seq_num < cons[conn_id]->expected_seq) {
                poli_tcp_ctrl_hdr ack;
                memset(&ack, 0, sizeof(poli_tcp_ctrl_hdr));
                ack.protocol_id = POLI_PROTOCOL_ID;
                ack.type = ACK_TYPE;
                ack.conn_id = conn_id;
                ack.recv_window = cons[conn_id]->max_window_seq;
                ack.ack_num = cons[conn_id]->expected_seq;
                sendto(cons[conn_id]->sockfd, &ack, sizeof(poli_tcp_ctrl_hdr), 0, (sockaddr *)&cons[conn_id]->servaddr, sizeof(sockaddr_in));
            } else if (data->seq_num >= cons[conn_id]->expected_seq && data->seq_num < cons[conn_id]->expected_seq + cons[conn_id]->max_window_seq) {
                if (cons[conn_id]->packets_buf.count(seq_num) == 0) {
                    cons[conn_id]->packets_buf[seq_num].assign(segment + sizeof(poli_tcp_data_hdr), segment + sizeof(poli_tcp_data_hdr) + data->len);
                }
                while (cons[conn_id]->packets_buf.count(cons[conn_id]->expected_seq) > 0) {
                    uint16_t next_seq = cons[conn_id]->expected_seq;
                    cons[conn_id]->recv_buf.insert(cons[conn_id]->recv_buf.end(), cons[conn_id]->packets_buf[next_seq].begin(), cons[conn_id]->packets_buf[next_seq].end());
                    cons[conn_id]->packets_buf.erase(next_seq);
                    cons[conn_id]->expected_seq++;
                }
                poli_tcp_ctrl_hdr ack;
                memset(&ack, 0, sizeof(poli_tcp_ctrl_hdr));
                ack.protocol_id = POLI_PROTOCOL_ID;
                ack.type = ACK_TYPE;
                ack.conn_id = conn_id;
                ack.recv_window = cons[conn_id]->max_window_seq;
                ack.ack_num = cons[conn_id]->expected_seq;
                sendto(cons[conn_id]->sockfd, &ack, sizeof(poli_tcp_ctrl_hdr), 0, (sockaddr *)&cons[conn_id]->servaddr, sizeof(sockaddr_in));
            }
        }
        pthread_mutex_unlock(&cons[conn_id]->con_lock);
    }

    
}

int wait4connect(uint32_t ip, uint16_t port)
{
    static int connection_id_cnt = 1;
    /* TODO: Implement the Three Way Handshake on the receiver part. This blocks
     * until a connection is established. */
    char buf[MAX_SEGMENT_SIZE];
    sockaddr_in client_addr;
    poli_tcp_ctrl_hdr *syn = (poli_tcp_ctrl_hdr *)buf;
    socklen_t addr_len = sizeof(client_addr);
    while(1) {
        int recv = recvfrom(main_sockfd, buf, MAX_SEGMENT_SIZE, 0, (struct sockaddr *)&client_addr, &addr_len);
        if (recv > 0) {
            if (syn->protocol_id == POLI_PROTOCOL_ID && syn->type == SYN_TYPE) {
                bool duplicate = false;
                for (auto con : cons) {
                    if (con.second->servaddr.sin_addr.s_addr == client_addr.sin_addr.s_addr && con.second->servaddr.sin_port == client_addr.sin_port) {
                        duplicate = true;
                        break;
                    }
                }
                if (!duplicate) {
                    break;
                }
            }
        }
    }
    struct connection *con = (struct connection *)malloc(sizeof(struct connection));
    new (con) connection();
    int conn_id = connection_id_cnt;
    connection_id_cnt++;
    con->servaddr = client_addr;
    con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);

    /* This can be used to set a timer on a socket, useful once we received a
     * SYN. You may want to disable by setting the time to 0 (tv_sec = 0,
     * tv_usec = 0) */

    /* Receive SYN on the connection socket. Create a new socket and bind it to
     * the chosen port. Send the data port number via SYN-ACK to the client */

    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 250000;
    if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO,&tv,sizeof(tv)) < 0) {
        perror("Error");
    }
    sockaddr_in local_addr;
    memset(&local_addr, 0, sizeof(local_addr));
    local_addr.sin_family = AF_INET;
    local_addr.sin_addr.s_addr = INADDR_ANY;
    local_addr.sin_port = 0;
    bind(con->sockfd, (struct sockaddr *)&local_addr, sizeof(local_addr));
    socklen_t local_addr_len = sizeof(local_addr);
    getsockname(con->sockfd, (struct sockaddr *)&local_addr, &local_addr_len);
    uint16_t dedicated_port = local_addr.sin_port;
    char response_buf[sizeof(poli_tcp_ctrl_hdr) + sizeof(uint16_t)];
    memset(response_buf, 0, sizeof(response_buf));
    poli_tcp_ctrl_hdr *syn_ack_hdr = (poli_tcp_ctrl_hdr *)response_buf;
    syn_ack_hdr->protocol_id = POLI_PROTOCOL_ID;
    syn_ack_hdr->conn_id = conn_id;
    syn_ack_hdr->type = SYN_ACK_TYPE;
    syn_ack_hdr->recv_window = server_recv_window / MAX_SEGMENT_SIZE;
    uint16_t *payload_port = (uint16_t *)(response_buf + sizeof(poli_tcp_ctrl_hdr));
    *payload_port = dedicated_port;
    while (1) {
        sendto(con->sockfd, response_buf, sizeof(response_buf), 0, (struct sockaddr *)&client_addr, sizeof(sockaddr_in));
        sockaddr_in recv_addr;
        socklen_t recv_addr_len = sizeof(recv_addr);
        int recv = recvfrom(con->sockfd, buf, MAX_SEGMENT_SIZE, 0, (struct sockaddr *)&recv_addr, &recv_addr_len);
        if (recv > 0) {
            poli_tcp_ctrl_hdr *confirm = (poli_tcp_ctrl_hdr *)buf;
            if (confirm->protocol_id == POLI_PROTOCOL_ID && confirm->type == ACK_TYPE && confirm->conn_id == conn_id) {
                break;
            }
        }
    }
    tv.tv_sec = 0;
    tv.tv_usec = 0;
    setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    con->conn_id = conn_id;
    con->max_window_seq = server_recv_window / MAX_SEGMENT_SIZE;
    con->expected_seq = 0;
    con->recv_buf.clear();
    con->packets_buf.clear();
    /* Since we can have multiple connection, we want to know if data is available
       on the socket used by a given connection. We use POLL for this */
    data_fds[fdmax].fd = con->sockfd;    
    data_fds[fdmax].events = POLLIN;    
    
    /* This creates a timer and sets it to trigger every 1 sec. We use this
       to know if a timeout has happend on a connection */
    timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME,  0);    
    timer_fds[fdmax].events = POLLIN;    
    struct itimerspec spec;     
    spec.it_value.tv_sec = 1;    
    spec.it_value.tv_nsec = 0;    
    spec.it_interval.tv_sec = 1;    
    spec.it_interval.tv_nsec = 0;    
    timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);    
    fdmax++;    

    pthread_mutex_init(&con->con_lock, NULL);
    cons.insert({conn_id, con});

    DEBUG_PRINT("Connection established!");

    return conn_id;
}

void init_receiver(int recv_buffer_bytes)
{
    pthread_t thread1;
    int ret;

    /* TODO: Create the connection socket and bind it to 8031 */
    server_recv_window = recv_buffer_bytes;
    main_sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    sockaddr_in main_sock_addr;
    memset(&main_sock_addr, 0, sizeof(sockaddr_in));
    main_sock_addr.sin_addr.s_addr = INADDR_ANY;
    main_sock_addr.sin_family = AF_INET;
    main_sock_addr.sin_port = htons(8031);
    bind(main_sockfd, (sockaddr *)&main_sock_addr, sizeof(sockaddr_in));
    ret = pthread_create( &thread1, NULL, receiver_handler, NULL);
    assert(ret == 0);
}
