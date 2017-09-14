#!/bin/sh
echo "Writing"

function run_test {
	n=$(($3+$4+$5))
	echo "---===  $1 $2 k=$3 r=$4 z=$5 rand=$6 n=$n servers=$7 ===---"
#    python parallel_platform.py $1 $2 $3 $4 $5 $6 $7 $n
}


function read_test {
	run_test read $1 $2 $3 $4 NONE $5
	run_test deg1 $1 $2 $3 $4 NONE $5
	run_test sf1 $1 $2 $3 $4 NONE $5
	if (($3>1)); then
		run_test deg2 $1 $2 $3 $4 NONE $5
		run_test sf2 $1 $2 $3 $4 NONE $5
	fi
       	cod=$1"_RA"
        run_test rcr $cod $2 $3 $4 NONE $5 $n
}

diffk=( 32 16 8 4 2 )
diffserv=( 37 23 13 11 7 )

for index in 0 1 2 3 4
do
	k=${diffk[$index]}
	servers=${diffserv[$index]}
	for re in 2 1
	do
		for z in 2 1
      		do
        		for cod in PSS BB
        		do
                    for rand in AES SIMPLE NONE
                    do
                        run_test write $cod $k $re $z $rand $servers
                    done
                    read_test $cod $k $re $z $servers
      			done
		done

		for cod in AES CHA AES_BC AONT_AES NONE
		do
            run_test write $cod $k $re 0 NONE $servers
            read_test $cod $k $re 0 $servers
		done
	done
done

k=1
cod=SSS
for z in 2 1
do
	for re in 2 1
	do 
        for rand in AES SIMPLE NONE
        do
            run_test write $cod $k $re $z $rand $servers
        done
        read_test $cod $k $re $z $servers
	done
done
