#ifndef SAGE_TIMER_HPP
#define SAGE_TIMER_HPP

#include <sys/time.h>

inline static float TimeDiff(struct timeval time1, struct timeval time2)
{
	return time2.tv_sec - time1.tv_sec
	       + ((float)(time2.tv_usec - time1.tv_usec))/1000000;
}

const std::string CurrentTime() {
	time_t now = time(0);
	struct tm tstruct;
	char buf[80];
	tstruct = *localtime(&now);
	strftime(buf, sizeof(buf), "%Y-%m-%d %X", &tstruct);

	return buf;
}
#endif //SAGE_TIMER_HPP
