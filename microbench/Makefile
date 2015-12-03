CPP=g++-4.8
#CPPFLAGS=-O2 -funroll-all-loops -g2 -DRTM_ENABLED
CPPFLAGS=-O2 -g2 -DRTM_ENABLED 
#CPPFLAGS=-g2 -DRTM_ENABLED

workingset : workingset.o   
	$(CPP) -o $@ $< -lstdc++ -lpthread -lrt

%.o : %.cc
	$(CPP) $(CPPFLAGS) -c -mrtm $< 

clean :
	rm -f *.o workingset cost treetest
