
# now, in test_def.h RATIO = 8
#bsub -a intelmpi -n 36 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 4 nc-out


# now, in test_def.h RATIO = 4
#mpirun -n 10 ./perform_test 4 2 nc-out 3 2
#mpirun -n 36 ./perform_test 8 4 nc-out 20 2


#36 8*4+8 
#72 8*8+8 
#108 8*12+12 
#144 8*16+16 
#180 8*20+20 
#216 8*24+24
#252 8*28+28
#288 8*32+32
#324 8*36+36
#360 8*40+40
#396 8*44+44
#432 8*48+48
#468 8*52+52
#504 8*56+56
#540 8*60+60
#576 8*64+64
#612 8*68+68
#648 8*72+72
#684 8*76+76

bsub -a intelmpi -n 36 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 4 nc-out 7 100
#bsub -a intelmpi -n 72 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 8 nc-out 100 2
#bsub -a intelmpi -n 108 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 12 8 nc-out 100 2
#bsub -a intelmpi -n 144 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 16 8 nc-out 100 2
#bsub -a intelmpi -n 180 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 20 8 nc-out 100 2
#bsub -a intelmpi -n 216 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 24 8 nc-out 100 2
#bsub -a intelmpi -n 252 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 28 8 nc-out 100 2
#bsub -a intelmpi -n 288 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 32 8 nc-out 100 2
#bsub -a intelmpi -n 324 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 36 8 nc-out 100 2
#bsub -a intelmpi -n 360 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 40 8 nc-out 100 2
#bsub -a intelmpi -n 396 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 44 8 nc-out 100 2
#bsub -a intelmpi -n 432 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 48 8 nc-out 100 2
#bsub -a intelmpi -n 468 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 52 8 nc-out 100 2
#bsub -a intelmpi -n 504 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 56 8 nc-out 100 2
#bsub -a intelmpi -n 540 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 60 8 nc-out 100 2
#bsub -a intelmpi -n 576 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 64 8 nc-out 100 2
#bsub -a intelmpi -n 612 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 68 8 nc-out 100 2
#bsub -a intelmpi -n 648 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 72 8 nc-out 100 2
#bsub -a intelmpi -n 684 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 76 8 nc-out 100 2


#args: lat lon out_dir itr sleep_itr
#bsub -a intelmpi -n 36 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 4 nc-out 1 0
#bsub -a intelmpi -n 36 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 4 nc-out 1 2
#bsub -a intelmpi -n 36 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 4 nc-out 1 4
#bsub -a intelmpi -n 36 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 4 nc-out 1 6
#bsub -a intelmpi -n 36 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 4 nc-out 1 8
#bsub -a intelmpi -n 36 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 4 nc-out 1 10
#
#bsub -a intelmpi -n 36 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 4 nc-out 10 0
#bsub -a intelmpi -n 36 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 4 nc-out 10 2
#bsub -a intelmpi -n 36 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 4 nc-out 10 4
#bsub -a intelmpi -n 36 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 4 nc-out 10 6
#bsub -a intelmpi -n 36 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 4 nc-out 10 8
#bsub -a intelmpi -n 36 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 4 nc-out 10 10
#
#bsub -a intelmpi -n 36 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 4 nc-out 50 0
#bsub -a intelmpi -n 36 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 4 nc-out 50 2
#bsub -a intelmpi -n 36 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 4 nc-out 50 4
#bsub -a intelmpi -n 36 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 4 nc-out 50 6
#bsub -a intelmpi -n 36 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 4 nc-out 50 8
#bsub -a intelmpi -n 36 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 4 nc-out 50 10
#
#bsub -a intelmpi -n 36 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 4 nc-out 100 0
#bsub -a intelmpi -n 36 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 4 nc-out 100 2
#bsub -a intelmpi -n 36 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 4 nc-out 100 4
#bsub -a intelmpi -n 36 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 4 nc-out 100 6
#bsub -a intelmpi -n 36 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 4 nc-out 100 8
#bsub -a intelmpi -n 36 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 4 nc-out 100 10
#
#bsub -a intelmpi -n 36 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 4 nc-out 150 0
#bsub -a intelmpi -n 36 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 4 nc-out 150 2
#bsub -a intelmpi -n 36 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 4 nc-out 150 4
#bsub -a intelmpi -n 36 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 4 nc-out 150 6
#bsub -a intelmpi -n 36 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 4 nc-out 150 8
#bsub -a intelmpi -n 36 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 4 nc-out 150 10
#
#bsub -a intelmpi -n 36 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 4 nc-out 200 0
#bsub -a intelmpi -n 36 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 4 nc-out 200 2
#bsub -a intelmpi -n 36 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 4 nc-out 200 4
#bsub -a intelmpi -n 36 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 4 nc-out 200 6
#bsub -a intelmpi -n 36 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 4 nc-out 200 8
#bsub -a intelmpi -n 36 -o pt-out/out.%J -e pt-err mpirun.lsf ./perform_test 8 4 nc-out 200 10
