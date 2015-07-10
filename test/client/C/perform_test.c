/****************************************************************************
 *       Filename:  perform_test.c
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  08/26/2012 10:09:11 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Wang Wencan 
 *	    Email:  never.wencan@gmail.com
 *        Company:  HPC Tsinghua
 ***************************************************************************/
#include <stdio.h>
#include <assert.h>

#include "mpi.h"
#include "cfio.h"
#include "debug.h"
#include "times.h"
#include "test_def.h"

int rank, size;
int LAT_PROC, LON_PROC;

void communication()
{
    double su[1024], sd[1024], sl[1024], sr[1024], ru[1024], rd[1024], rl[1024], rr[1024];
    int up, down, left, right, x, y, i;
    MPI_Request reqs[8];
    MPI_Status stas[8];

    for (i = 0; i < 1024; ++i) {
	su[i] = sd[i] = sl[i] = sr[i] = 1.0;
    }

    x = rank % LAT_PROC;
    y = rank / LAT_PROC;

    up = LAT_PROC * ((y + 1) % LON_PROC) + x;
    down = LAT_PROC * ((y - 1 + LON_PROC) % LON_PROC) + x;
    left = LAT_PROC * y + (x - 1 + LAT_PROC) % LAT_PROC;
    right = LAT_PROC * y + (x + 1) % LAT_PROC;

    MPI_Isend(su, 1024, MPI_DOUBLE, up, up, MPI_COMM_WORLD, &reqs[0]);
    MPI_Isend(sd, 1024, MPI_DOUBLE, down, down, MPI_COMM_WORLD, &reqs[1]);
    MPI_Isend(sl, 1024, MPI_DOUBLE, left, left, MPI_COMM_WORLD, &reqs[2]);
    MPI_Isend(sr, 1024, MPI_DOUBLE, right, right, MPI_COMM_WORLD, &reqs[3]);

    MPI_Irecv(ru, 1024, MPI_DOUBLE, up, rank, MPI_COMM_WORLD, &reqs[4]);
    MPI_Irecv(rd, 1024, MPI_DOUBLE, down, rank, MPI_COMM_WORLD, &reqs[5]);
    MPI_Irecv(rl, 1024, MPI_DOUBLE, left, rank, MPI_COMM_WORLD, &reqs[6]);
    MPI_Irecv(rr, 1024, MPI_DOUBLE, right, rank, MPI_COMM_WORLD, &reqs[7]);

    for (i = 0; i < 8; ++i) {
	MPI_Wait(&reqs[i], &stas[i]);
    }
}

int main(int argc, char** argv)
{
    int ncidp;
    int dim1,var1,i, j, l;

    size_t start[2],count[2];
    char fileName[100];
    char var_name[16];
    int *var;
    int valn;

    double compute_time = 0.0, 
	   IO_time = 0.0, 
	   communicate_time = 0.0, 
	   waitIO_time = 0.0, 
	   init_time = 0.0, 
	   final_time = 0.0, 
	   total_time = 0.0;

    int itr, sleep_ms_per_itr, ratio;

    size_t len = 10;
    MPI_Comm comm = MPI_COMM_WORLD;

    if(8 != argc)
    {
	printf("Usage : perform_test LAT_PROC LON_PROC output_dir itr sleep_ms_per_itr\n");
	return -1;
    }
    
    LAT_PROC = atoi(argv[1]);
    LON_PROC = atoi(argv[2]);
    ratio = atoi(argv[3]);

    itr = atoi(argv[5]);
    sleep_ms_per_itr = atoi(argv[6]);
    valn = atoi(argv[7]);
    
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(comm, &rank);
    MPI_Comm_size(comm, &size);

    // print args
    if (0 == rank) {
	printf("LAT = %d \n LON = %d \n VALN = %d \n LAT_PROC = %d \n LON_PROC = %d \n RATIO = %d \n  \
		ITR = %d \n SLEEP_TIME = %d \n SLEEP_MS_PER_ITR = %d \n ", 
		LAT, LON, valn, LAT_PROC, LON_PROC, ratio, itr, SLEEP_TIME, sleep_ms_per_itr);
    }

    times_init();
    double start_time = times_cur();

    start[0] = (rank % LAT_PROC) * (LAT / LAT_PROC);
    start[1] = (rank / LAT_PROC) * (LON / LON_PROC);
    count[0] = LAT / LAT_PROC;
    count[1] = LON / LON_PROC;
    double *fp = malloc(count[0] * count[1] *sizeof(double));

    for( i = 0; i< count[0] * count[1]; i++) {
	fp[i] = i + rank * count[0] * count[1];
    }

    times_start(); //total_time

    times_start(); // init_time
    cfio_init( LAT_PROC, LON_PROC, ratio);
    init_time = times_end(); // init_time

    CFIO_START();
    double start_time = times_cur();

    var = malloc(sizeof(int) * valn);

    for(i = 0; i < itr; i ++)
    {
	times_start();
	if( SLEEP_TIME != 0)
	{
	    struct timeval sleep_begin, sleep_end;
	    // simulate computation of sleep_ms_per_itr ms
	    gettimeofday(&sleep_begin, NULL);
	    do {
		gettimeofday(&sleep_end, NULL);
	    } while ((sleep_end.tv_sec - sleep_begin.tv_sec) * 1000 + (sleep_end.tv_usec - sleep_begin.tv_usec) / 1000 < sleep_ms_per_itr);
	}
	compute_time += times_end();
	if (i > 2) { // record only itr 3 - N
	    times_start(); // communicate_time
	}
	sprintf(fileName,"%s/cfio-%d.nc", argv[4], i);
	int dimids[2];
	cfio_create(fileName, NC_64BIT_OFFSET, &ncidp);
	int lat = LAT;
	cfio_def_dim(ncidp, "lat", LAT,&dimids[0]);
	cfio_def_dim(ncidp, "lon", LON,&dimids[1]);
	//cfio_put_att(ncidp, NC_GLOBAL, "global", NC_CHAR, 6, "global");

	for(j = 0; j < valn; j++)
	{
	    sprintf(var_name, "time_v%d", j);
	    cfio_def_var(ncidp,var_name, CFIO_DOUBLE, 2,dimids, 
		    start, count, &var[j]);
	//    cfio_put_att(ncidp, var[j], "global", NC_CHAR, 
	//	    strlen(var_name), var_name );
	}
	cfio_enddef(ncidp);

	for(j = 0; j < valn; j++)
	{
	    cfio_put_vara_double(ncidp,var[j], 2,start, count,fp);
	}
	//cfio_put_vara_float(rank,ncidp,var1, 2,start, count,fp); 
	//cfio_put_vara_float(rank,ncidp,var1, 2,start, count,fp); 

	cfio_close(ncidp);
	if (i > 2) {
	    communicate_time += times_end(); // communicate_time
	}
	if (i > 2) {
	    times_start(); // waitIO_time
	}
	cfio_io_end();
	if (i > 2) {
	    waitIO_time += times_end(); // waitIO_time
	}
    }
    free(fp);

    if (var) {
	free(var);
    }

    CFIO_END();

    times_start(); // final_time
    cfio_finalize();
    final_time = times_end(); // final_time

    total_time = times_end();

    if (0 == rank) {
	printf("%8s %8s %8s %8s %8s %8s %8s %8s %8s\n", "proc", 
		"init", "final", "total", "compute", "communi", "waitIO", "IO", "simulate"); 
    }
    printf("%8d %8d %8d %8d %8d %8d %8d %8d %8d\n", rank, 
	    (int)init_time, 
	    (int)final_time, 
	    (int)total_time,
	    (int)compute_time, 
	    (int)communicate_time, 
	    (int)waitIO_time, 
	    (int)(communicate_time + waitIO_time), 
	    (int)(total_time - init_time - final_time));
    
    MPI_Finalize();
    
    times_final();
    return 0;
}
