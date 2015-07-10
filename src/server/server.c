/****************************************************************************
 *       Filename:  server.c
 *
 *    Description:  main program for server
 *
 *        Version:  1.0
 *        Created:  03/13/2012 02:42:11 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Wang Wencan 
 *	    Email:  never.wencan@gmail.com
 *        Company:  HPC Tsinghua
 ***************************************************************************/
#include <pthread.h>
#include <string.h>

#include "server.h"
#include "recv.h"
#include "io.h"
#include "id.h"
#include "mpi.h"
#include "debug.h"
#include "times.h"
#include "define.h"
#include "cfio_error.h"

/* the thread read the buffer and write to the real io node */
static pthread_t writer;
/* the thread listen to the mpi message and put data into buffer */
static pthread_t reader;
/* my real rank in mpi_comm_world */
static int rank;
static int nprocs;
static int server_proc_num;	    /* server group size */

static int reader_done, writer_done;

// zc
static int recv_num = 0;
static int overlap_io_num = 0;
static int io_num = 0;
#ifdef TEST_ED
static double *handle_times;
#endif 
// zc

static int decode(cfio_msg_t *msg)
{	
    int client_id = 0;
    int ret = 0;
    uint32_t code;
    /* TODO the buf control here may have some bad thing */
    cfio_recv_unpack_func_code(msg, &code);
    client_id = msg->src;

    switch(code)
    {
	case FUNC_NC_CREATE: 
	    debug(DEBUG_SERVER,"server %d recv nc_create from client %d",
		    rank, client_id);
	    cfio_io_create(msg);
	    debug(DEBUG_SERVER, "server %d done nc_create for client %d\n",
		    rank,client_id);
	    return CFIO_ERROR_NONE;
	case FUNC_NC_DEF_DIM:
	    debug(DEBUG_SERVER,"server %d recv nc_def_dim from client %d",
		    rank, client_id);
	    cfio_io_def_dim(msg);
	    debug(DEBUG_SERVER, "server %d done nc_def_dim for client %d\n",
		    rank,client_id);
	    return CFIO_ERROR_NONE;
	case FUNC_NC_DEF_VAR:
	    debug(DEBUG_SERVER,"server %d recv nc_def_var from client %d",
		    rank, client_id);
	    cfio_io_def_var(msg);
	    debug(DEBUG_SERVER, "server %d done nc_def_var for client %d\n",
		    rank,client_id);
	    return CFIO_ERROR_NONE;
	case FUNC_PUT_ATT:
	    debug(DEBUG_SERVER, "server %d recv nc_put_att from client %d",
		    rank, client_id);
	    cfio_io_put_att(msg);
	    debug(DEBUG_SERVER, "server %d done nc_put_att from client %d",
		    rank, client_id);
	    return CFIO_ERROR_NONE;
	case FUNC_NC_ENDDEF:
	    debug(DEBUG_SERVER,"server %d recv nc_enddef from client %d",
		    rank, client_id);
	    cfio_io_enddef(msg);
	    debug(DEBUG_SERVER, "server %d done nc_enddef for client %d\n",
		    rank,client_id);
	    return CFIO_ERROR_NONE;
	case FUNC_NC_PUT_VARA:
	    debug(DEBUG_SERVER,"server %d recv nc_put_vara from client %d",
		    rank, client_id);
	    cfio_io_put_vara(msg);
	    debug(DEBUG_SERVER, 
		    "server %d done nc_put_vara_float from client %d\n", 
		    rank, client_id);
	    return CFIO_ERROR_NONE;
	case FUNC_NC_CLOSE:
	    debug(DEBUG_SERVER,"server %d recv nc_close from client %d",
		    rank, client_id);
	    cfio_io_close(msg);
	    debug(DEBUG_SERVER,"server %d received nc_close from client %d\n",
		    rank, client_id);
	    return CFIO_ERROR_NONE;
	case FUNC_FINAL:
	    debug(DEBUG_SERVER,"server %d recv client_end_io from client %d",
		    rank, msg->src);
	    cfio_io_reader_done(msg->src, &reader_done);
	    debug(DEBUG_SERVER, "server %d done client_end_io for client %d\n",
		    rank,msg->src);
	    return CFIO_ERROR_NONE;
	default:
	    error("server %d received unexpected msg from client %d",
		    rank, client_id);
	    return CFIO_ERROR_UNEXPECTED_MSG;
    }	
}

static void * cfio_reader(void *argv)
{
    int ret = 0;
    cfio_msg_t *msg;
    
    while(!reader_done)
    {
        msg = cfio_recv_get_first();
        if(NULL != msg)
        {
            decode(msg);
	    free(msg);
        }
    }
    
    debug(DEBUG_SERVER, "Server(%d) Reader done", rank);
    return ((void *)0);
}
static inline void process_one(int client_num)
{
    int i;
    cfio_msg_t *msg;

    for(i = 0; i < client_num; i++)
    {
	msg = cfio_recv_get_first();
	decode(msg);
	free(msg);
	io_num++;
    }
}

static void* cfio_writer(void *argv)
{
    cfio_msg_t *msg;
    int i, server_index, client_num;
    uint32_t func_code;
    int *client_id;
    double comm_time = 0.0, overlap_IO_time = 0.0, IO_time = 0.0, decode_time = 0.0, get_msg_time = 0.0;
    double start_time = times_cur();
    int decode_num, flag;

    server_index = cfio_map_get_server_index(rank);
    client_num = cfio_map_get_client_num_of_server(rank);
    client_id = malloc(sizeof(int) * client_num);
    if(client_id == NULL)
    {
	error("malloc fail.");
	return (void*)0;
    }
    cfio_map_get_clients(rank, client_id);

#ifdef TEST_ED // event-driven
    int itr = 1;
    int msg_order = 1;
    char test_msg[40];
    MPI_Status sta;
    handle_times = malloc(sizeof(double) * client_num);
#endif // event-driven

    while(!writer_done)
    {

#ifdef TEST_ED // event-driven
	if (1 == msg_order && 2 == itr) {
	    printf("proc %d begin test-ed \n", rank);
	    for (i = 0; i < client_num; ++i) {
		MPI_Recv(test_msg, 40, MPI_CHAR, client_id[i], client_id[i], MPI_COMM_WORLD, &sta);
		handle_times[i] = times_cur() - base_time;
	    }
	    printf("proc %d end test-ed \n", rank);
	}
	msg_order++;
#endif // event-driven
	
	/*  recv from client one by one, to make sure that data recv and output in time */
	times_start();
	for(i = 0; i < client_num; i ++)
	{
	    while(cfio_recv(client_id[i], rank, cfio_map_get_comm(), &func_code)
		    == CFIO_RECV_BUF_FULL)
	    {
		times_start();
		process_one(client_num);
		IO_time += times_end();
	    }

	    // zc
	    recv_num++;
	    // zc
	    
	    if(func_code == FUNC_FINAL)
	    {
		debug(DEBUG_SERVER,"server(writer) %d recv client_end_io from client %d",
			rank, client_id[i]);
		cfio_io_writer_done(client_id[i], &writer_done);
		debug(DEBUG_SERVER, "server(writer) %d done client_end_io for client %d\n",
			rank,client_id[i]);
	    }
	}
	
	// zc
// 	if (nprocs - 1 == rank) 
// 	    printf("%f: server %d comm. \n", times_cur(), rank);
	// zc

	comm_time += times_end();
	//msg = cfio_recv_get_first();
	//while(NULL != msg)
	//{
	//    decode(msg);
	//    free(msg);
	//    msg = cfio_recv_get_first();
	//}
	if(func_code == FUNC_IO_END)
	{
#ifdef TEST_ED // event-driven
	    printf("proc %d itr %d recv_num %d msg_order %d \n", rank, itr, recv_num, msg_order);
	    itr++;
	    msg_order = 1;
#endif // event-driven

	    times_start();

	    //printf("Server %d recv point : %f\n", rank, times_cur() - start_time);
	    decode_num = 0;
	    msg = cfio_recv_get_first();
	    //times_start();
	    while(NULL != msg)
	    {

		// zc
//		if (nprocs - 1 == rank) 
//		    printf("%f: server %d handle a msg. \n", times_cur(), rank);
		overlap_io_num++;
		// zc

		decode(msg);
		free(msg);
		decode_num ++;
		if(decode_num == client_num)
		{
		    cfio_iprobe(client_id, client_num, cfio_map_get_comm(), &flag);
		    if(flag == 1) // has recv arrived , recv first
		    {
		    //    printf("Server %d iprobe true : %f\n", rank, times_cur() - start_time);
		        break;
		    }
		    decode_num = 0;
		}
		msg = cfio_recv_get_first();
	    }
	    //printf("Server %d one loop time : %f\n", rank, times_end());

	    overlap_IO_time += times_end();
	}
    }
	times_start();

    // zc
//     if (nprocs - 1 == rank) 
// 	printf("%f: server %d begin final IO. \n", times_cur(), rank);
    // zc
	
    msg = cfio_recv_get_first();
    while(NULL != msg)
    {

	// zc
//	if (nprocs - 1 == rank)
//	    printf("%f: server %d handle a msg. \n", times_cur(), rank);
	io_num++;
	// zc

	times_start(); // zc
	decode(msg);
	decode_time += times_end(); // zc

	free(msg);

	times_start(); // zc
	msg = cfio_recv_get_first();
	get_msg_time += times_end(); // zc
    }

	IO_time += times_end();

    // zc
	// printf("%f: server %d end final IO. \n", times_cur(), rank);
	printf("%f: server %d , recv_num %d, overlap_io_num %d, io_num %d. \n", times_cur(), rank, recv_num, overlap_io_num, io_num);
	// printf("%f: Server %d comm time : %f\n", times_cur(), rank, comm_time);
	printf("%f: Server %d overlap_IO_time : %f\n", times_cur(), rank, overlap_IO_time);
	printf("%f: Server %d IO_time : %f\n", times_cur(), rank, IO_time);
	printf("%f: Server %d overall_IO_time : %f\n", times_cur(), rank, IO_time + overlap_IO_time);
	// printf("%f: Server %d decode_time : %f\n", times_cur(), rank, decode_time);
	// printf("%f: Server %d get_msg_time : %f\n", times_cur(), rank, get_msg_time);
    // zc
	
    //printf("Server %d comm time : %f\n", rank, comm_time);
    //printf("Server %d pnetcdf time : %f\n", rank, IO_time);
    //printf("Server end : %f\n", times_cur() - start_time);
    debug(DEBUG_SERVER, "Server(%d) Writer done", rank);
    return ((void *)0);
}

int cfio_server_start()
{
    int ret = 0;
    double server_start_time = 0.0;

    times_start();
    cfio_writer((void*)0);
    server_start_time += times_end();
    printf("%f: Server %d server_start_time: %f\n", times_cur(), rank, server_start_time);

#ifdef TEST_ED // event-driven
    int client_num = cfio_map_get_client_num_of_server(rank);
    int *client_id = malloc(sizeof(int) * client_num);
    cfio_map_get_clients(rank, client_id);

    double *base_times, *ready_times, *finish_times;
    base_times = malloc(sizeof(double) * client_num);
    ready_times = malloc(sizeof(double) * client_num);
    finish_times = malloc(sizeof(double) * client_num);

    int i;
    MPI_Status sta;
    for (i = 0; i < client_num; ++i) {
	// MPI_Recv(&base_times[i], 1, MPI_DOUBLE, client_id[i], 0, MPI_COMM_WORLD, &sta);
	MPI_Recv(&ready_times[i], 1, MPI_DOUBLE, client_id[i], 1, MPI_COMM_WORLD, &sta);
	MPI_Recv(&finish_times[i], 1, MPI_DOUBLE, client_id[i], 2, MPI_COMM_WORLD, &sta);
    }

    int min_ready_clt = 0;
    for (i = 1; i < client_num; ++i) {
	if (ready_times[i] < ready_times[min_ready_clt]) {
	    min_ready_clt = i; 
	}
    }

    double idle_wait = ready_times[0] - ready_times[min_ready_clt];

    for (i = 1; i < client_num; ++i) {
	if (finish_times[i - 1] < ready_times[i]) {
	    idle_wait += ready_times[i] - finish_times[i - 1];
	}
    }

    double latency_svr = 0, latency_clt = 0;
    for (i = 0; i < client_num; ++i) {
	latency_svr += handle_times[i] - ready_times[i];
	latency_clt += finish_times[i] - ready_times[i];
    }

    printf("event-driven proc %d idle_wait %f latency_svr %f avg %f latency_clt %f avg %f \n", 
	    rank, idle_wait, latency_svr, latency_svr / client_num, latency_clt, latency_clt / client_num);

    free(client_id);
    free(base_times);
    free(ready_times);
    free(finish_times);
    free(handle_times);
#endif // event-driven



    return CFIO_ERROR_NONE;
}

int cfio_server_init()
{
    int ret = 0;
    int x_proc_num, y_proc_num;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

    if((ret = cfio_recv_init()) < 0)
    {
	error("");
	return ret;
    }
    
    reader_done = 0;
    writer_done = 0;
    
    if((ret = cfio_id_init(CFIO_ID_INIT_SERVER)) < 0)
    {
	error("");
	return ret;
    }

    if((ret = cfio_io_init(rank)) < 0)
    {
	error("");
	return ret;
    }

times_init();

    return CFIO_ERROR_NONE;
}

int cfio_server_final()
{
    cfio_io_final();
    cfio_id_final();
    cfio_recv_final();

times_final();

    return CFIO_ERROR_NONE;
}
