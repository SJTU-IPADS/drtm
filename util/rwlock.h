#ifndef LEVELDB_RWLOCK_H
#define LEVELDB_RWLOCK_H

#include <stdint.h>

/* The counter should be initialized to be 0. */
namespace leveldb {

class RWLock  {

private:
  volatile uint16_t counter;

public:

  RWLock(){ counter = 0;}
  
  void StartWrite();
  void EndWrite();

  void StartRead();
  void EndRead();

};

}
#endif /* _RWLOCK_H */
