#ifndef GRAPH_ENGINE_THREAD_H
#define GRAPH_ENGINE_THREAD_H

#include <pthread.h>

class Thread
{
private:
	pthread_t myThread;
	unsigned int tid;

	static void *_Run (void *pthis_) {
		Thread *pthis = (Thread *)pthis_;
		pthis->Run ();
		pthread_exit (NULL);
	}

public:
	Thread (int tid = 0){
		this->tid = tid;
	}

	~Thread(){};

	virtual void Run () = 0;

	void Start(){
		pthread_create(&myThread, NULL, _Run, (void *) this);
	}

	void Join(){
		pthread_join(myThread, NULL);
	}

	int GetTid() {
		return tid;
	}
};


#endif //GRAPH_ENGINE_THREAD_H
