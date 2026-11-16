#include <pthread.h>
#include <cstdlib>
#include <map>
#include <cstdint>
#include "lib.h"
#include "utils.h"
#include "protocol.h"
#include <cassert>
#include <poll.h>
#include <sys/timerfd.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>
#include <new>

using namespace std;

std::map<int, struct connection *> cons;

struct pollfd data_fds[MAX_CONNECTIONS];
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;

static int initial_window_size = 0;

static void send_packet_by_seq(struct connection *con, uint16_t seq_num) {
    int offset = 0;
    for (uint16_t seq = con->send_base; seq != seq_num; seq++) {
        offset = offset + con->sent_packet_sizes[seq];
    }
    int chunk_size = 0;
    if (con->sent_packet_sizes.count(seq_num) > 0) {
        chunk_size = con->sent_packet_sizes[seq_num];
    } else {
        int remaining_bytes = (int)con->send_buf.size() - offset;
        if (remaining_bytes <= 0){ 
            return;
        } else if (remaining_bytes < MAX_DATA_SIZE) {
            chunk_size = remaining_bytes;
        } else {
            chunk_size = MAX_DATA_SIZE;
        }
        con->sent_packet_sizes[seq_num] = chunk_size;
    }
    char buf[MAX_SEGMENT_SIZE];
    poli_tcp_data_hdr *hdr = (poli_tcp_data_hdr *)buf;
    hdr->protocol_id = POLI_PROTOCOL_ID;
    hdr->conn_id = con->conn_id;
    hdr->type = DATA_TYPE;
    hdr->seq_num = seq_num;
    hdr->len = chunk_size;
    memcpy(buf + sizeof(poli_tcp_data_hdr), con->send_buf.data() + offset, chunk_size);
    sendto(con->sockfd, buf, sizeof(poli_tcp_data_hdr) + chunk_size, 0, (struct sockaddr *)&con->servaddr, sizeof(sockaddr_in));
}

int send_data(int conn_id, char *buffer, int len)
{
    int size = 0;

    pthread_mutex_lock(&cons[conn_id]->con_lock);

    /* We will write code here as to not have sync problems with sender_handler */
    cons[conn_id]->send_buf.insert(cons[conn_id]->send_buf.end(), buffer, buffer + len);
    while (cons[conn_id]->next_seq < cons[conn_id]->send_base + cons[conn_id]->max_window_seq) {
        int offset = 0;
        for (uint16_t seq = cons[conn_id]->send_base; seq != cons[conn_id]->next_seq; seq++) {
            offset = offset + cons[conn_id]->sent_packet_sizes[seq];
        }
        int remaining_bytes = (int)cons[conn_id]->send_buf.size() - offset;
        if (remaining_bytes <= 0) {
            break;
        }
        send_packet_by_seq(cons[conn_id], cons[conn_id]->next_seq);
        cons[conn_id]->next_seq++;
    }

    pthread_mutex_unlock(&cons[conn_id]->con_lock);

    size = len;
    return size;
}

void *sender_handler(void *arg)
{
    int res = 0;
    char buf[MAX_SEGMENT_SIZE];

    while (1) {

        if (cons.size() == 0) {
            usleep(2000);
            continue;
        }
        int conn_id = -1;
        do {
            res = recv_message_or_timeout(buf, MAX_SEGMENT_SIZE, &conn_id);
        } while(res == -14);

        pthread_mutex_lock(&cons[conn_id]->con_lock);

        /* Handle segment received from the receiver. We use this between locks
        as to not have synchronization issues with the send_data calls which are
        on the main thread */
        struct connection *con = cons[conn_id];
        if (res > 0) {
            poli_tcp_ctrl_hdr *ack = (poli_tcp_ctrl_hdr *)buf;
            if (ack->protocol_id == POLI_PROTOCOL_ID && ack->type == ACK_TYPE) {
                uint16_t ack_value = ack->ack_num;
                if (ack_value > con->send_base) {
                    int total_bytes_to_erase = 0;
                    for (uint16_t seq = con->send_base; seq != ack_value; seq++) {
                        total_bytes_to_erase = total_bytes_to_erase + con->sent_packet_sizes[seq];
                        con->sent_packet_sizes.erase(seq);
                    }
                    con->send_buf.erase(con->send_buf.begin(), con->send_buf.begin() + total_bytes_to_erase);
                    con->send_base = ack_value;
                    con->duplicate_ack_count = 0;
                } 
                else if (ack_value == con->send_base) {
                    con->duplicate_ack_count++;
                    if (con->duplicate_ack_count == 3) {
                        if (con->sent_packet_sizes.count(ack_value) > 0) {
                            send_packet_by_seq(con, ack_value);
                        }
                    }
                }
            }
        }
        else if (res == -1) {
            if (con->next_seq > con->send_base) {
                send_packet_by_seq(con, con->send_base);
            }
        }
        while (con->next_seq < con->send_base + con->max_window_seq) {
            int offset = 0;
            for (uint16_t seq = con->send_base; seq != con->next_seq; seq++) {
                offset = offset + con->sent_packet_sizes[seq];
            }
            int remaining_bytes = (int)con->send_buf.size() - offset;
            if (remaining_bytes <= 0) {
                break;
            }
            send_packet_by_seq(con, con->next_seq);
            con->next_seq++;
        }

        pthread_mutex_unlock(&cons[conn_id]->con_lock);
    }
    return NULL;
}

int setup_connection(uint32_t ip, uint16_t port)
{
    /* Implement the sender part of the Three Way Handshake. Blocks
    until the connection is established */

    struct connection *con = new connection();
    int conn_id = 0;
    con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    memset(&con->servaddr, 0, sizeof(sockaddr_in));
    con->servaddr.sin_family = AF_INET;
    con->servaddr.sin_port = htons(8031);
    con->servaddr.sin_addr.s_addr = ip;
    
    /* This can be used to set a timer on a socket */
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 250000;
    if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO,&tv,sizeof(tv)) < 0) {
        perror("Error");
    }
    poli_tcp_ctrl_hdr syn;
    memset(&syn, 0, sizeof(poli_tcp_ctrl_hdr));
    syn.protocol_id = POLI_PROTOCOL_ID;
    syn.type = SYN_TYPE;

    /* We will send the SYN on 8031. Then we will receive a SYN-ACK with the connection
     * port. We can use con->sockfd for both cases, but we will need to update server_addr
     * with the port received via SYN-ACK */

    char buf[MAX_SEGMENT_SIZE];
    sockaddr_in recv_addr;
    poli_tcp_ctrl_hdr *syn_ack = (poli_tcp_ctrl_hdr *)buf;
    socklen_t slen = sizeof(recv_addr);
    while(1) {
        sendto(con->sockfd, &syn, sizeof(poli_tcp_ctrl_hdr), 0, (struct sockaddr *) &con->servaddr, sizeof(sockaddr_in));
        int recv = recvfrom(con->sockfd, buf, MAX_SEGMENT_SIZE, 0, (struct sockaddr *)&recv_addr, &slen);
        if (recv > 0) {
            if (syn_ack->protocol_id == POLI_PROTOCOL_ID && syn_ack->type == SYN_ACK_TYPE) {
                conn_id = syn_ack->conn_id;
                break;
            }
        }
    }
    tv.tv_sec = 0;
    tv.tv_usec = 0;
    setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    con->servaddr.sin_port = *(uint16_t *)(buf + sizeof(poli_tcp_ctrl_hdr));
    poli_tcp_ctrl_hdr ack;
    memset(&ack, 0, sizeof(poli_tcp_ctrl_hdr));
    ack.protocol_id = POLI_PROTOCOL_ID;
    ack.conn_id = conn_id;
    ack.type = ACK_TYPE;
    for (int i = 0; i < 3; i++) {
        sendto(con->sockfd, &ack, sizeof(poli_tcp_ctrl_hdr), 0, (struct sockaddr *) &con->servaddr, sizeof(sockaddr_in));
        usleep(10000);
    }
    con->max_window_seq = initial_window_size / MAX_SEGMENT_SIZE;
    if (con->max_window_seq > (int) syn_ack->recv_window) {
        con->max_window_seq = syn_ack->recv_window;
    }
    con->conn_id = conn_id;
    con->send_base = 0;
    con->next_seq = 0;
    con->send_buf.clear();
    con->sent_packet_sizes.clear();
    con->duplicate_ack_count = 0;
    /* Since we can have multiple connection, we want to know if data is available
       on the socket used by a given connection. We use POLL for this */
    data_fds[fdmax].fd = con->sockfd;    
    data_fds[fdmax].events = POLLIN;    
    
    /* This creates a timer and sets it to trigger every 1 sec. We use this
       to know if a timeout has happend on our connection */
    timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME,  0);    
    timer_fds[fdmax].events = POLLIN;    
    struct itimerspec spec;     
    spec.it_value.tv_sec = 0;    
    spec.it_value.tv_nsec = 50000000;    
    spec.it_interval.tv_sec = 0;    
    spec.it_interval.tv_nsec = 50000000;    
    timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);    
    fdmax++;

    pthread_mutex_init(&con->con_lock, NULL);
    cons.insert({conn_id, con});

    DEBUG_PRINT("Connection established!");

    return conn_id;
}

void init_sender(int speed, int delay)
{
    pthread_t thread1;
    int ret;
    initial_window_size = (((speed * 1000000) / 8) * 2 * delay) / 1000;
    /* Create a thread that will*/
    ret = pthread_create( &thread1, NULL, sender_handler, NULL);
    assert(ret == 0);
}
