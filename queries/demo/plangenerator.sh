for file in *Pig*.txt
do
    # create directory
    echo "Processing " $file
    dirname=`basename $file .txt`
    #echo $dirname
    mkdir -p plans/$dirname

    :> output.pig
    :>error.pig

    # calculate plans
    commands="EXPLAIN -dot -script $file -out plans/$dirname/;"
    echo $commands i | pig -x local >>output.pig 2>>error.pig
    
    # rename dot files
    cd plans/$dirname
    mv *logical*.dot $dirname-logical_plan.dot
    mv *exec*.dot $dirname-exec_plan.dot
    mv *physical*.dot $dirname-physical_plan.dot

    dot -Tpdf $dirname-logical_plan.dot -o $dirname-logical_plan.pdf
    dot -Tpdf $dirname-exec_plan.dot -o $dirname-exec_plan.pdf
    dot -Tpdf $dirname-physical_plan.dot -o $dirname-physical_plan.pdf
    
    mv *.pdf ../ 
    rm *.dot
    cd -
    rm -rf plans/$dirname

    rm *.log
done
