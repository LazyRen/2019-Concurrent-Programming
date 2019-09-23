#!/bin/csh
setenv TBBROOT "/home/lazyren/concurrent_programming/project1/lib/tbb" #
setenv tbb_bin "/home/lazyren/concurrent_programming/project1/lib/tbb/build/linux_intel64_gcc_cc7.4.0_libc2.27_kernel5.0.0_debug" #
if (! $?CPATH) then #
    setenv CPATH "${TBBROOT}/include" #
else #
    setenv CPATH "${TBBROOT}/include:$CPATH" #
endif #
if (! $?LIBRARY_PATH) then #
    setenv LIBRARY_PATH "${tbb_bin}" #
else #
    setenv LIBRARY_PATH "${tbb_bin}:$LIBRARY_PATH" #
endif #
if (! $?LD_LIBRARY_PATH) then #
    setenv LD_LIBRARY_PATH "${tbb_bin}" #
else #
    setenv LD_LIBRARY_PATH "${tbb_bin}:$LD_LIBRARY_PATH" #
endif #
 #
