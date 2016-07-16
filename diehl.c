/* Diehl Interface program */

#include <stdio.h>	// for FILE
#include <stdlib.h>	// for timeval
#include <string.h>	// for strlen etc
#include <time.h>	// for ctime
#include <sys/types.h>	// for fd_set
// #include <sys/socket.h>
// #include <netinet/in.h>
#include <netdb.h>	// for sockaddr_in 
#include <fcntl.h>	// for O_RDWR
#include <termios.h>	// for termios
#include <unistd.h>		// for getopt
#ifdef linux
#include <errno.h>		// for Linux
#endif
#include <sys/mman.h>	// for PROT_READ
#include <sys/stat.h>	// for struct stat
#include <getopt.h>

#include "../Common/common.h"
#include "ccitt.h"

#define PROGNAME "Diehl"
const char progname[] = "Diehl";
#define LOGFILE "/tmp/diehl%d.log"
#define LOGON "diehl"
#define BAUD B19200
#define MAXINVERTERS 45

/*  1.1 07/10/2010 Initial version copied from Resol 2.4 (via Evoco)
	1.2 26/11/2010 Better testing fields: -a (address) -t (test value). Use of diehlX.dat for list of inverters.
	1.3 13/06/2011 Modifed to handle RS485 echo on xuart1.  
			Look for list of PlatinumBus objects in /root/diehl%d.conf then /root/diehl.conf
 Added Bus Reset and Ack Busy handling
15 Jul 2011 Modified on site Azur 
 Added -w to scale watts by 10
	1.4 2011-07-31 Increate MAXINVERTERS To 45 for Bodyshop
	1.5 2011-08-15 Bugfix in Event handling, multipart, state handling
	1.6 2011-08-23 Allow dotted format for serial number in diehlX.dat files. (Read & write)
    1.7 2011-09-05 Improve detection
    1.8 2011-09020 Increase Describe buffer to 40
    1.9 2011-09-21 -U for units support 
	1.10 2011-10-07 Show "2.3" or "5" for device ni depengin on unseUnits. -U toggles hasUnits
	1.11 2011-10-12 Treat states 12 and 13 as the same to reduce number of messages
	1.12 2011-11-25 Added T4 (Temp UI Max) and improved RED LED handling
	1.13 2012-12-19 Crashes with 3 x Diehl 3100S at Margaret McMillan
	1.14 2012-04-11 Added V4.8 error and event messages
 */

static char* id="@(#)$Id: diehl.c,v 1.14 2012/04/11 18:47:47 martin Exp $";
#define REVISION "$Revision: 1.14 $"

#define PORTNO 10010
#define SERIALNAME "/dev/ttyAM0"	/* although it MUST be supplied on command line */

// Severity levels.  ERROR and FATAL terminate program
#define INFO	0
#define	WARN	1
#define	ERROR	2
#define	FATAL	3
// Socket retry params
#define NUMRETRIES 3
int numretries = NUMRETRIES;
#define RETRYDELAY	1000000	/* microseconds */
int retrydelay = RETRYDELAY;
// Elster values expected
#define MAXPARAMS 45    /* From Platinum-Bus Data Objects */
// Set to if(0) to disable debugging
#define DEBUGFP stderr   /* set to stderr or logfp as required */
// Serial retry params
#define SERIALNUMRETRIES 10
#define SERIALRETRYDELAY 1000000 /*microseconds */
// If defined, use stdin
// #define DEBUGCOMMS
#define SRCADDR 0x4c0b2501
#define CMDSEARCH 0xe000

/* SOCKET CLIENT */

/* Command line params: 
 1 - device name
 2 - device timeout. Default to 60 seconds
 */

#ifndef linux
extern
#endif
int errno;  
// Override Clib definition of swab (swap bytes)
#define swab(x) (((x&0xff) << 8) | ((x&0xFF00) >> 8))

typedef unsigned short u16;
typedef unsigned char uchar;

// Procedures in this file
int processSocket(void);			// process server message
void usage(void);					// standard usage message
char * getversion(void);
int getbuf(int max);	// get a buffer full of message
// time_t timeMod(time_t t);
void dumpbuf(int start);		// Dump out a buffer for debug
void sendCommand(int cmd, int pktnum, char * params, int len);
int synctostart();		// wait for 0xAA in the input. 0 == success.
#define STARTSYNC 0x16
int isPort(char *);				// Decide if connection to hostname:port
void setupSendMsg(int dest, int src);	// Setup send buffer with address
void processPacket(int currentInv);			// Output it all
void stateMessage(int state, int inverter);
int detect(int commfd, int maxinverters);
u16 fcs16(u16 fcs, uchar * cp, int len);
void readfile();
void writefile();
struct platbus;		// Forward declaration
void fillSent(struct platbus * p);		// platbus{} -> sent{}
void readconf();		// read diehl%d.conf
void processData(int start, int currentInv);
void identify(int num);	// nice message
void reset(int commfd);		// Bus reset
void processPart(int start);
void displayMultipart();
void eventMessage(int event, int currentInv);
void unescape();		// Strip out escape 0x10
char * formatSerial(int serial);		// Display a serial number string
int find_code(int code);
int hasecho(int fd);
int readSerial(char * data);			// Read in x.x.x.x.x.x format

/* GLOBALS */
FILE * logfp = NULL;
int sockfd[MAXINVERTERS];	// Because openSockets expects an array.
int inverter[MAXINVERTERS];	// The address of inverter[i]
int numinverters;
int currentInv;
int debug = 0;
short test = 0;
int noserver = 0;		// prevents socket connection when set to 1
int cmd;
int echo = -1;			// Initially unknown
int useUnits = 1;			// Using sub-units?

// Common Serial Framework
#define BUFSIZE 2048	/* should be longer than max possible line of text */
struct data {	// The serial buffer
	int count;
	unsigned char buf[BUFSIZE];
	int escape;		// Count the escapes in this message
	int sentlength;
} data;

struct sent {
	int num;		// How many params
	int length;		// Length of response
	short param[MAXPARAMS];
} sent;

struct multipart {
	int count;		// Initialise to 0x80
	int limit;		// Set when 0x8? response received
	char * ptr;		// pointer to data
	
	char buf[8192];
	char filter;	} multipart;

unsigned char sendBuf[100];

int controllernum = -1;	//	only used in logon message
int gap =  0;		//How many characters to treat as 0 in detect()

#define debugfp stderr
#define INITFCS 0xFFFF
#define GOODFCS 0x1D0F

char buffer[256];
char * serialName = SERIALNAME;
int srcaddr = SRCADDR;

int commfd;	// The serial port.
int maxinverters = 1;

enum type {NONE, BYTE, WORD, LONG, STRING};

struct platbus {
	unsigned short code;
	int length;		// length in bytes. Can't tell difference between STRING3 and LONG.
	enum type type;
	int inuse;		// 1 = used
	float scale;	// Division factor
	char format[12];	// for example "kwh:%.1f"
} platbus[] = {
	0x0001, 4, LONG, 0,	1,	"",	// CTYPE_SERIAL serial number in hex 00 2A 6B 34
	0x0002, 1, BYTE, 0,	1,	"",	// CTYPE_LANGUAGE
	0x0003, 1, BYTE, 0,	1,	"",	// CTYPE_COUNTRY
	0x0004, 4, STRING, 0, 1,	"",	// CTYPE_CURRENCY
	0x0005, 4, LONG, 0,	1,	"",	// CTYPE_COMPENSATION
	0x0006, 2, WORD, 0,	1,	"",	// CTYPE_CO2_FACTOR
//  0x0007, ?, ????, 0, 1,  "", // CTYPE_CONSTANT_VOLTAGE
	0x0008, 4, LONG, 0,	1,	"",	// CTYPE_TIME - Unix timestamp
//  0x0009, ?, ????, 0, 1,  "", // CTYPE_TIMEZONE
	0x000A, 1, BYTE, 0,	1,	"",	// CTYPE_NUMBER - number (from 1 upwards)
	0x000B, 1, BYTE, 1,	1,	"",	// CTYPE_STATUS
	0x000C, 2, WORD, 1,	1,	"",	// CTYPE_EVENT - Event information
	0x080C, 0, NONE, 0,	1,	"",	// CTYPE_EVENT2 - not on Diehl4300
	0x000D, 0, NONE, 0,	1,	"",	// CTYPE_EVENT_SOURE - not Diehl4300
	0x100E, 2, WORD, 0, 10,	"t1:%.1f", // CTYPE_TEMPERATURE1
	0x200E, 2, WORD, 0, 10,	"t2:%.1f", // CTYPE_TEMPERATURE2
	0x300E, 2, WORD, 0, 10,	"t3:%.1f", // CTYPE_TEMPERATURE3
	0x400E, 2, WORD, 0, 10,	"t4:%.1f", // CTYPE_TEMPERATURE4
	0x000F, 2, WORD, 0, 1,	"", // CTYPE_TEMPERATURE5
	0x0010, 2, WORD, 0, 1,	"", // CTYPE_TEMPERATURE6
	0x0011, 2, WORD, 1, 1,		"vdc:%.1f",		// CTYPE_DC_VOLTAGE
	0x0014, 2, WORD, 1, 10.0,	"idc:%.2f",		// CTYPE_DC_CURRENT
	0x0017, 2, WORD, 1, 1,		"wdc:%.1f",		// CTYPE_DC_POWER  MTR Changed from 10 to 1
	0x001A, 2, WORD, 1, 1,		"vac:%.1f",		// CTYPE_AC_VOLTAGE
	0x001D, 2, WORD, 1, 10.0,	"iac:%.2f",		// CTYPE_AC_CURRENT
	0x0020, 2, WORD, 1, 1,		"watts:%.1f",	// CTYPE_AC_POWER  MTR Changed from 10 to 1
	0x0023, 2, WORD, 0, 100.0,	"hz:%.2f",		// CTYPE_FREQUENCY
	0x1023, 2, WORD, 0, 100.0,	"hz:%.2f",		// CTYPE_FREQUENCY_L1
	0x5023, 2, WORD, 0, 100.0,	"hz2:%.2f",		// CTYPE_FREQUENCY_L2
	0x9023, 2, WORD, 0, 100.0,	"hz3:%.2f",		// CTYPE_FREQUENCY_L3
	0x0024, 4, LONG, 0, 1,		"",		// CTYPE_ENERGY_TODAY
	0x0025, 4, LONG, 1, 1000.0,	"kwh:%.3f",		// CTYPE_ENERGY_TODAY
	0x0026, 2, WORD, 0, 1,	"",		// CTYPE_OPTIMEDAY
	0x0027, 4, LONG, 1, 60,	"ophr:%.2f",		// CTYPE_OPTIMETOTAL  (in minutes)
	0x0028,19, STRING, 0, 1,	"",		// CTYPE_PLANT_NAME
	0x0029,19, STRING, 0, 1,	"",		// CTYPE_PLANT_DESCR
//  0x002A, ?, ????, 0, 1,  "",			// CTYPE_INVERTER_LOCK -- DO NOT USE!	
//  0x002B, ?, ????, 0, 1,  "",			// CTYPE_ISOLATION_RESISTANCE
//  0x0031, ?, ????, 0, 1,  "",			// CTYPE_AFI_WERT fault current instananseous value
//  0x0035, ?, ????, 0, 1,  "",			// CTYPE_STROM_ACDC - DC on AC side instantaneous value		
	0x0042, 9, STRING, 0, 1,	"",		// CTYPE_DEVICE_FAMILY
	0x0043, 7, STRING, 0, 1,	"",		// CTYPE_DEVICE_TYPE
	0x0044,15, STRING, 0, 1,	"",		// CTYPE_DEVICE_IDENTIFIER
	0x0045, 3, STRING, 0, 1,	"",		// CTYPE_DEVICE_POWERCLASS
//  0x0046, ?, ????, 0, 1,  "",			// CTYPE_DEVICE_PHASING
	0x0052, 2, WORD, 1, 1,	"vac:%.1f",		// CTYPE_AC_VOLTAGE_L1
	0x0056, 2, WORD, 0, 1,	"vac2:%.1f",	// CTYPE_AC_VOLTAGE_L2
	0x005A, 2, WORD, 0, 1,	"vac3:%.1f",	// CTYPE_AC_VOLTAGE_L3
	0x005E, 2, WORD, 1, 10,	"iac:%.2f",		// CTYPE_AC_CURRENT_L3
	0x0061, 2, WORD, 0, 10,	"iac2:%.2f",	// CTYPE_AC_CURRENT_L3
	0x0064, 2, WORD, 0, 10,	"iac3:%.2f",	// CTYPE_AC_CURRENT_L3
//  0x0067, ?, ????, 0, 1,  "",				// CTYPE_SUMMERWINTERTIME	
//  0x006F, ?, ????, 0, 1,  "",			// CTYPE_DATETIME_FORMAT
	0,0,0,0,1.0,""};				// END VALUE

struct platbus identify_list[] = {
	0x0004, 4, STRING, 1, 1,	"",	// CTYPE_CURRENCY
	0x0028,19, STRING, 1, 1,	"",		// CTYPE_PLANT_NAME
	0x0029,19, STRING, 1, 1,	"",		// CTYPE_PLANT_DESCR
	0x0042, 9, STRING, 1, 1,	"",		// CTYPE_DEVICE_FAMILY
	0x0043, 7, STRING, 1, 1,	"",		// CTYPE_DEVICE_TYPE
	0x0044,15, STRING, 0, 1,	"",		// CTYPE_DEVICE_IDENTIFIER
	0x0045, 2, STRING, 0, 1,	"",		// CTYPE_DEVICE_POWERCLASS
	0,0,0,0,1.0,""};				// END VALUE
	
/********/
/* MAIN */
/********/
int main(int argc, char *argv[])
// arg1: serial device file
// arg2: optional timeout in seconds, default 60
{
	int nolog = 0;
	time_t next;
	int address = 0;
	int run = 1;		// set to 0 to stop main loop
	fd_set readfd, errorfd; 
	int numfds;
	struct timeval timeout;
	int tmout = 10;
	int logerror = 0;
	int online = 1;		// used to prevent messages every minute in the event of disconnection
	int option; 
	int dodetect = 1;
	int rts = 0;

	// Command line arguments
	bzero(data.buf, BUFSIZE);
	data.count = 0;
	opterr = 0;
	
	while ((option = getopt(argc, argv, "DUsl?dVZn:Na:t:m:wTReEg:")) != -1) {
		switch (option) {
			case 'U': useUnits = !useUnits;	break;
			case 's': noserver = 1; break;
			case 'l': nolog = 1; break;
			case '?': usage(); exit(1);
			case 'd': debug++; break;
			case 'D': dodetect = 0;	break;
			case 'n': maxinverters = atoi(optarg); break;
			case 'V': printf("Version %s %s\n", getversion(), id); exit(0);
			case 'N': break;	// Ignore it.
			case 'a': address = strtol(optarg, NULL, 0);  break;
			case 't': test = strtol(optarg, NULL, 0); 
				test = swab(test);
				break;
			case 'w': platbus[find_code(0x17)].scale = 10.0;
				platbus[find_code(0x20)].scale = 10.0;
				break;
			case 'm': srcaddr = strtol(optarg, NULL, 0);  break;
			case 'e':	echo = 1; break;
			case 'E': echo = 0;  break;
			case 'R': rts = 1; break;	// Disable RTS
			case 'g': gap = strtol(optarg, NULL, 0); break;
			case 'Z': decode("(b+#Gjv~z`mcx-@ndd`rxbwcl9Vox=,/\x10\x17\x0e\x11\x14\x15\x11\x0b\x1a" 
							 "\x19\x1a\x13\x0cx@NEEZ\\F\\ER\\\x19YTLDWQ'a-1d()#!/#(-9' >q\"!;=?51-??r"); exit(0);
		}
	}
	
	DEBUG printf("Debug on %d. optind %d argc %d\n", debug, optind, argc);
	
	if (optind < argc) serialName = argv[optind];		// get seria/device name: parameter 1
	optind++;
	if (optind < argc) controllernum = atoi(argv[optind]);	// get optional controller number: parameter 2
	
	sprintf(buffer, LOGFILE, controllernum);
	
	if (!nolog) if ((logfp = fopen(buffer, "a")) == NULL) logerror = errno;	
	
	// There is no point in logging the failure to open the logfile
	// to the logfile, and the socket is not yet open.
	
	sprintf(buffer, "STARTED %s on %s as %d timeout %d %s", argv[0], serialName, controllernum, tmout, nolog ? "nolog" : "");
	logmsg(INFO, buffer);
	
	if (maxinverters == 0) exit(0);		// in particular don't try and lock serial device
	
	useUnits = openSockets(0, maxinverters, "inverter", REVISION, PROGNAME, useUnits);
	
	// Open serial port
	if ((commfd = openSerial(serialName, BAUD, 0, CS8, 1)) < 0) {
		sprintf(buffer, "FATAL " PROGNAME " %d Failed to open %s: %s", controllernum, serialName, strerror(errno));
#ifdef DEBUGCOMMS
		logmsg(INFO, buffer);			// FIXME AFTER TEST
		fprintf(stderr, "Using stdio\n");
		commfd = 0;		// use stdin
#else
		logmsg(FATAL, buffer);
#endif
	}
	DEBUG2 fprintf(stderr,"Commfd %d ", commfd);
	blinkLED(0, REDLED);

	if (rts) {
		disable_rts(commfd);
		DEBUG fprintf(stderr, "Disabling RTS ");
	}
	
#ifndef DEBUGCOMMS
	if (flock(commfd, LOCK_EX | LOCK_NB) == -1) {
		sprintf(buffer, "FATAL " PROGNAME " %d is already running, cannot start another one on %s", controllernum,  serialName);
		logmsg(FATAL, buffer);
	}
#endif
	
	// If we failed to open the logfile and were NOT called with nolog, warn server
	// Obviously don't use logmsg!
	if (logfp == NULL && nolog == 0) {
		sprintf(buffer, "event WARN " PROGNAME " %d could not open logfile %s: %s", controllernum, LOGFILE, strerror(logerror));
		sockSend(sockfd[0], buffer);
	}
	
	numfds = (sockfd[0] > commfd ? sockfd[0] : commfd) + 1;		// nfds parameter to select. One more than highest descriptor
	
	
	if (echo == -1)
		echo = hasecho(commfd);
	DEBUG fprintf(stderr,"Echotest: %d\n", echo);
	
	readfile();		// diehlX.dat - list of inverters
	readconf();		// diehlX.conf or diehl.conf - list of parameters
	
	// Start with detecting inverters unless address specified as -a or -D (Don't detect) used
	if (dodetect) {
		if (! address) {
			reset(commfd);
			
			while (detect(commfd, maxinverters)) {
				DEBUG fprintf(stderr,"Detect failed .. pause 60 sec\n");
				sleep(60);
			}
			// Never writefile unless requested to do so, ie using detect.
			writefile();
			if (numinverters > maxinverters && useUnits) {	// Send additional logins
				DEBUG fprintf(stderr, "\nIncreasing units from %d to %d - sending units command\n", maxinverters, numinverters);
				sprintf(buffer, "units %d", numinverters);
				sockSend(sockfd[0], buffer);
				DEBUG fprintf(stderr, "Additional sign on from %d to %d\n", maxinverters, numinverters);
				useUnits = openSockets(maxinverters, numinverters, "inverter", REVISION, PROGNAME, useUnits);
						// Additional sign on messages
			}
		}
		else {		// -a XXX means overwrite everything - use only single address specified
			numinverters = 1;
			inverter[0] = address;
		}
	}
	if (!dodetect && address) {		// -D -a ...  (Don't detect; use this address)
		numinverters = 1;
		inverter[0] = address;
	}
	
	if (numinverters > maxinverters) {
		sprintf(buffer, "ERROR " PROGNAME " %d %d inverters found, %d allowed. Use -n %d",
		controllernum, numinverters, maxinverters, numinverters);
		logmsg(ERROR, buffer);
	}
	
	// Main Loop
	FD_ZERO(&readfd); 
	FD_ZERO(&errorfd); 
	reset(commfd);
	
	next = 0;	// so we always get one at startup.
	DEBUG2 fprintf(stderr, "Now is %zd next is %zd test is %d\n", time(NULL), next, test);
	
	currentInv = 0;
	cmd = 0xF2;		// Initial value.
	fillSent(platbus);
	while(run) {
		timeout.tv_sec = tmout;
		timeout.tv_usec = 0;
		FD_SET(commfd, &readfd);
		FD_SET(commfd, &errorfd);
		DEBUG3 fprintf(stderr, "Before: Readfd %lx ", readfd);
		DEBUG3 fprintf(stderr, "ErrorFD %lx ", errorfd);
		blinkLED(0, REDLED);
		DEBUG fprintf(stderr,"Using inverter[%d] = %d (0x%0x) ", currentInv, inverter[currentInv], inverter[currentInv]);
		
		// data.count = 0;

		setupSendMsg(inverter[currentInv], srcaddr);	// The address
		switch(cmd) {
			case 0xF2:		// Data Values
				if (test)
					sendCommand(0xF2, 0, (char *)&test, 2);	// Although test is an int, send the low two bytes.
				else
					sendCommand(0xF2, 0, (char *)&sent.param[0], sent.num * 2);	
				break;
			case 0xF3:		// Data DESC
			case 0x73:
				sendCommand(0xF3, multipart.count, &multipart.filter, 1);		break;
			default:
				fprintf(stderr, "OOPS - not coded for Command 0x%x\n", cmd);
		}

		if (select(numfds, &readfd, NULL, &errorfd, &timeout) == 0) {	// select timed out. Bad news 
			if (online == 1) {
				sprintf(buffer, "WARN " PROGNAME " %s No data for last period", unitStr(controllernum, currentInv, useUnits));
				logmsg(WARN, buffer);
				online = 0;	// Don't send a message every minute from now on
			}
			// For VBus, attempt to reconnect
			if (isPort(serialName)) {
				close(commfd);
				if ((commfd = openSerial(serialName, BAUD, 0, CS8, 1)) < 0) {
					sprintf(buffer, "WARN " PROGNAME " %d Failed to reopen %s", controllernum, serialName);
					logmsg(WARN, buffer);
				}
			}
			goto next;
		}
		DEBUG3 fprintf(stderr, "After: Readfd %lx ", readfd);
		DEBUG3 fprintf(stderr, "ErrorFD %lx ", errorfd);
		if (FD_ISSET(commfd, &readfd)) {
			data.count = 0;
			data.escape = 0;
			synctostart(commfd);
			getbuf(180);	// Was 55, then 78 
			unescape();
			processPacket(currentInv);
		};
		blinkLED(0, REDLED);
		
	next:		
		// Set to next inverter;
		DEBUG2 fprintf(stderr, "\nCurrinv %d Numinverters %d ", currentInv, numinverters);
		currentInv++;
		if (currentInv >= numinverters) {
			currentInv = 0;
			sleep(8);
		}
		
		timeout.tv_sec = 0;
		timeout.tv_usec = 0;
		FD_SET(sockfd[0], &readfd);
		FD_SET(sockfd[0], &errorfd);
		if (select(sockfd[0] + 1, &readfd, NULL, &errorfd, &timeout)) {
		
			// Need to check for socket readable separately.
			// Reason: Send a command.  Before any serial data comes back the socket
			// might be readable, so it the select() completes. 
			run = processSocket();	// the server may request a shutdown by setting run to 0
		}
		// Next command

	}
	sprintf(buffer, "INFO " PROGNAME " %d Shutdown requested", controllernum);
	logmsg(INFO, buffer);
	close(sockfd[0]);
	closeSerial(commfd);
	
	return 0;
}

/*********/
/* USAGE */
/*********/
void usage(void) {
	printf("Usage: %s [-t timeout] [-l] [-s] [-f value] [-d] [-DRUVNO] [-w] [-e|E|T] [-m myaddress] [-a address] [-t register] [-g chars] /dev/ttyname controllernum\n", progname);
	printf("-l: no log  -s: no server  -d: debug on\n -V: version -i: interval in seconds\n");
	printf("-N New format -O old format -D Don't detect -U toggle use of subints -w Scale watts x 10 -R disable RTS\n");
	printf("-e no RS485 echo -E force RS485 echo (default) -T test for RS485 echo -g gap to reat as 1\n");
	return;
}

/*****************/
/* PROCESSSOCKET */
/*****************/
int processSocket(){
	// Deal with commands from MCP.  Return to 0 to do a shutdown
	short int msglen, numread;
	char buffer2[192];	// about 128 is good but rather excessive since longest message is 'truncate'
	char * cp = &buffer[0];
	int retries = NUMRETRIES;
	
	if (read(sockfd[0], &msglen, 2) != 2) {
		sprintf(buffer2, "WARN " PROGNAME " %d Failed to read length from socket", controllernum);
		logmsg(WARN, buffer2);
		return 1;
	}
	msglen =  ntohs(msglen);
	while ((numread = read(sockfd[0], cp, msglen)) < msglen) {
		cp += numread;
		msglen -= numread;
		if (--retries == 0) {
			sprintf(buffer2, "WARN " PROGNAME " %d Timed out reading from server", controllernum);
			logmsg(WARN, buffer2);			return 1;
		}
		usleep(RETRYDELAY);
	}
	cp[numread] = '\0';	// terminate the buffer 
	
	if (strcmp(buffer, "exit") == 0)
		return 0;	// Terminate program
	if (strcmp(buffer, "Ok") == 0)
		return 1;	// Just acknowledgement
	if (strcmp(buffer, "truncate") == 0) {
		if (logfp) {
			// ftruncate(logfp, 0L);
			// lseek(logfp, 0L, SEEK_SET);
			freopen(NULL, "w", logfp);
			sprintf(buffer2, "INFO " PROGNAME " %d Truncated log file", controllernum);
			logmsg(INFO, buffer2);
		} else {
			sprintf(buffer2, "INFO " PROGNAME " %d Log file not truncated as it is not open", controllernum);
			logmsg(INFO, buffer2);
		}
		
		return 1;
	}
	if (strcmp(buffer, "debug 0") == 0) {	// turn off debug
		debug = 0;
		return 1;
	}
	if (strcmp(buffer, "debug 1") == 0) {	// enable debugging
		debug = 1;
		return 1;
	}
	if (strcmp(buffer, "debug 2") == 0) {	// enable debugging
		debug = 2;
		return 1;
	}
	if (strcmp(buffer, "clear") == 0) {	// reset to no inverters known and call detect
		numinverters = 0;
		detect(commfd, maxinverters);
	}
	if (strncmp(buffer, "describe", 8) == 0) {
		int n;
		n = strtol(buffer + 9, NULL, 0);
		if (n >= numinverters) {
			logmsg(WARN, "Invalid inverter number in Describe command");
			return 1;
		}
		setupSendMsg(inverter[n], SRCADDR);
		cmd = 0xF3;
		multipart.filter = 0x78;
		multipart.count = 0x80;
		return 1;
	}
	if (strcmp(buffer, "help") == 0) {
		sprintf(buffer2, "INFO " PROGNAME " %d Commands are: debug 0|1|2, exit, truncate, clear, describe N", controllernum);
		logmsg(INFO, buffer2);
		return 1;
	}
	
	sprintf(buffer2, "INFO " PROGNAME " Unknown message from server: ");
	strcat(buffer2, buffer);
	logmsg(INFO, buffer2);	// Risk of loop: sending unknown message straight back to server
	
	return 1;	
};

/**************/
/* GETVERSION */
/**************/
char *getversion(void) {
	// return pointer to version part of REVISION macro
	static char version[10] = "";	// Room for xxxx.yyyy
	if (!strlen(version)) {
		strcpy(version, REVISION+11);
		version[strlen(version)-2] = '\0';
	}
	return version;
}

/***********/
/* DUMPBUF */
/***********/
void dumpbuf(int start) {
	int i;
	for (i = start; i < data.count; i++) {
		fprintf(stderr, "%02x", data.buf[i]);
		if (i == 3)
			putc('-', stderr);
		else
			putc(' ', stderr);
	}
	putc('\n', stderr);
}

/**********/
/* GETBUF */
/**********/
int getbuf(int max) {
	// Read up to max chars into supplied buf. Return number
	// of chars read or negative error code if applicable
	// Remember there may be escapes.
	
	// This version has 0x10 as the escape.  But only in the Data section.  It's far too hard to
	// handle escapses inline so get the whole buffer and worry about it later.
	
	int ready, numtoread, now;
	fd_set readfd; 
	struct timeval timeout;
	FD_ZERO(&readfd);
	data.escape = 0;
	// numread = 0;
	numtoread = max;
	DEBUG2 fprintf(stderr, "Getbuf max=%d currently=%d ", max ,data.count);
	
	while(1) {
		// overflow protection
		if (data.count == BUFSIZE - 1) {
			sprintf(buffer, "WARN " PROGNAME " %d Getbuf: Buffer overflow",  currentInv);
			logmsg(WARN, buffer);
			data.count = 0;
			return 0;
		}
		FD_SET(commfd, &readfd);
		timeout.tv_sec = 0;
		timeout.tv_usec = 500000;	 // 0.5sec
		ready = select(commfd + 1, &readfd, NULL, NULL, &timeout);
		DEBUG4 {
			gettimeofday(&timeout, NULL);
			fprintf(stderr, "%03ld.%03d ", timeout.tv_sec%100, timeout.tv_usec / 1000);
		}
		if (ready == 0) {
			DEBUG2 fprintf(stderr, "Gotbuf %d bytes ", data.count);
			return data.count;		// timed out - return what we've got
		}
		DEBUG4 fprintf(stderr, "Getbuf: before read1 ");
		now = read(commfd, data.buf + data.count, 1);
		DEBUG4 fprintf(stderr, "After read1\n");
		DEBUG3 fprintf(stderr, "0x%02x ", data.buf[data.count]);
		if (now < 0)
			return now;
		if (now == 0) {
			fprintf(stderr, "ERROR fd was ready but got no data\n");
			// VBUs / LAN  - can't use standard Reopenserial as device name hostname: port is not valid
			commfd = reopenSerial(commfd, serialName, BAUD, 0, CS8, 1);
			continue;
		}
		
		data.count += now;
		numtoread -= now;
		DEBUG3 fprintf(stderr, "[%d] ", data.count - now);
		if (numtoread == 0) return data.count;
		if (numtoread < 0) {	// CANT HAPPEN
			fprintf(stderr, "ERROR buffer overflow - increase max from %d (numtoread = %d numread = %d)\n", 
					max, numtoread, data.count);
			return data.count;
		}
	}
}

/************/
/* UNESCAPE */
/************/
void unescape() { 
	// Starting with the last 0x16 0x10 0x02 sequencee, remove double 0x10.
	unsigned char * src, *dest;
	int n = 0;
	
	// overflow detection
	if (data.count < 3) {
		sprintf(buffer, "WARN " PROGNAME " trying to unescape when count = %d", data.count);
		logmsg(WARN, buffer);
		return;
	}
	src = data.buf + data.count - 3;
	// search back for 0x16 0x10 0x02
	DEBUG fprintf(stderr,"Unescape: data.count=%d ", data.count);
	while( (src > data.buf) && !((*src == 0x16) && (*(src+1) == 0x10) && (*(src+2) == 0x02))) src--, n++;
	
	// Overflow detection
	if (src < data.buf) {
		sprintf(buffer, "WARN " PROGNAME " unescape: before start of buffer");
		src = data.buf;
		logmsg(WARN, buffer);
		return;
	}
	DEBUG2 fprintf(stderr, "Looking at %02x %02x %02x\n", *src, *(src+1), *(src+2));
	src += 3;	// point to start of address
	dest = src;
	DEBUG2 fprintf(stderr, "Data.buf=0x%0x src=0x%0x n= %d ", data.buf, src, n); 
	while (src < data.buf + data.count - 4) {
		if (*src == 0x10) src++;		//skip it
		*dest++ = *src++;
	}
	while (src < data.buf + data.count) *dest++ = *src++;
	DEBUG2 fprintf(stderr, "after: dest=0x%0x src=0x%0x ", dest, src);
	data.count += dest - src;
	DEBUG fprintf(stderr,"Data.count now %d\n", data.count);
	
	// Overflow detection
	if (data.count >= BUFSIZE) {
		sprintf(buffer, "WARN " PROGNAME " unescape: data.count = %d", data.count);
		logmsg(WARN, buffer);
		data.count = BUFSIZE;
		return;
	}
}
	
/*****************/
/* SYNC TO Start */
/*****************/
int synctostart(int fd) {
	// This will wait for AA characters with a 10-sec timeout
	fd_set readfd, errorfd; 
	struct timeval timeout;
	int numread, ready;
	FD_ZERO(&readfd);
	FD_ZERO(&errorfd);
	numread = 0;
	DEBUG2 fprintf(stderr, "Synctostart fd %d ", fd);
	while(1) {
		FD_SET(fd, &readfd);
		FD_SET(fd, &errorfd);
		timeout.tv_sec = 10;
		timeout.tv_usec = 0;	 // 10 sec
		errno = 0;
		ready = select(fd + 1, &readfd, NULL, &errorfd, &timeout);
		if (ready == 0) {	// timed out - return error
			DEBUG fprintf(stderr, "SyncToStart - timedout. errno %d\n", errno);
			return -1;
		}
		DEBUG4 fprintf(stderr, "Sync: readfd = %x ", readfd);
		DEBUG4 fprintf(stderr, "errorfd = %lx ", errorfd);
		DEBUG4 fprintf(stderr, "ready = %x ", ready);
		ready = read(fd, data.buf, 1);
		if (ready <= 0) {
			DEBUG fprintf(stderr, "FD ready but read returns %d (%s)\n", 
						  ready, strerror(ready));	// Comms error - missing device?
			fd = reopenSerial(fd, serialName, BAUD, 0, CS8, 1);
			
			return ready;
		}
		if (data.buf[0] == STARTSYNC) {
			DEBUG3 fprintf(stderr, "Sync skipped %d bytes .. ", numread);
			data.count = 1;
			return 0;	// success
		}
		numread++;
	}
}

/**************/
/* SENDSERIAL */
/**************/
int sendSerial(int fd, unsigned char data) {
	// Send a single byte.  Return 1 for a logged failure
	int retries = SERIALNUMRETRIES;
	int written;
#ifdef DEBUGCOMMS
	printf("Comm 0x%02x(%d) ", data, data);
	return 0;
#endif
	
	DEBUG2 fprintf(DEBUGFP, "%02x.", data);
	while ((written = write(fd, &data, 1)) < 1) {
        fprintf(DEBUGFP, "Serial wrote %d bytes errno = %d", written, errno);
        perror("");
		if (--retries == 0) {
			sprintf(buffer, "WARN " PROGNAME " %d timed out writing to serial port", controllernum);
			logmsg(WARN, buffer);
			return 1;
		}
		DEBUG fprintf(DEBUGFP, "Pausing %d ... ", SERIALRETRYDELAY);
		usleep(SERIALRETRYDELAY);
	}
	return 0;       // ok
}

// NOTE 7-bit version!
#define lobyte(x) (x & 0x7f)
#define hibyte(x) ((x >> 8) & 0x7F)


/*********/
/* FCS16 */
/*********/
u16 fcs16(u16 fcs, uchar * cp, int len) {
	// If return from this is GOODFCS when on entry FCS = INITFCS, the packet is valid
	while (len--) {
		DEBUG3 fprintf(stderr, "VAL %x ", *cp);
		fcs = (fcs << 8) ^ crc_table[((fcs >>8) ^ *cp++) & 0xFF];
		// DEBUG fprintf(stderr, "CRC=%04x ", fcs);
	}
	return fcs;
}

/***************/
/* SENDCOMMAND */
/***************/
void sendCommand(int cmd, int pktnum, char * params, int cmdlen)
{
	// As before, return 1 for a logged failure, otherwise 0
	// Send from sendMsg buffer
	
	// Special case: cmd = 0: resend buffer exactly as is
	
	int i, fcs;
	static int len;
	
	DEBUG {
		fprintf(DEBUGFP, "SendCommand: %02x %02x ", cmd, pktnum);
	}
			
	if (cmd) {	// If CMD == 0, resend as is
		len = cmdlen;
		sendBuf[8] = cmd;
		sendBuf[9] = pktnum;
		for (i = 0; i < len; i++) {
			sendBuf[10 + i] = params[i];
		}
	}

	DEBUG {
		for (i = 0; i < len; i++)
		fprintf(DEBUGFP, "%02x ", params[i]);
		fprintf(DEBUGFP,"-- ");
	}
	
	fcs = INITFCS;
	fcs = fcs16(fcs, sendBuf, 10 + len);
	fcs ^= 0xffff;		// Get complement of CRC.
	
	// Send message
	sendSerial(commfd, STARTSYNC);
	// STX sequence
	sendSerial(commfd, 0x10);
	sendSerial(commfd, 0x02);
	
	data.sentlength = 0;
	
	// Only escape data in the DATA section of the message - not the checksum
	for (i = 0; i < 10 + len; i++) {
		if (sendBuf[i] == 0x10) {
			sendSerial(commfd, 0x10);
			data.sentlength++;
		}
		sendSerial(commfd, sendBuf[i]);
	}
	// ETX sequence
	sendSerial(commfd, 0x10);
	sendSerial(commfd, 0x03);
	// Checksum
	sendSerial(commfd, (fcs >>8) & 0xff);
	sendSerial(commfd, fcs & 0xff);
	
	data.sentlength += 17 + len;
	// Deduct 2 for the escaped 0x10 02 and 0x10 03.
	DEBUG2 fprintf(stderr,"Sentlength = %d ", data.sentlength);
}

/* These differ from the SMA function of the same name as the Diehl is big-endian  */
inline int getShort(int offset) {
	return data.buf[offset+1] | (data.buf[offset] << 8);
}

inline int getLong(int offset) {
	return data.buf[offset+3] | (data.buf[offset+2] << 8) | (data.buf[offset+1] << 16) |  (data.buf[offset] << 24);
}

int isPort(char * name) {
	// Return TRUE if name is of form hostname:port
	// else FALSE
	if (strchr(name, ':'))
		return 1;
	else
		return 0;
}

/******************/
/* SETUP SEND MSG */
/******************/
void setupSendMsg(int dest, int src) {
	// Assume no parameters
	bzero(sendBuf, sizeof(sendBuf));
	unsigned char chk;
	chk = 0;
	sendBuf[0] = (dest >> 24) & 0xff;
	sendBuf[1] = (dest >> 16) & 0xff;
	sendBuf[2] = (dest >> 8) & 0xff;
	sendBuf[3] = (dest) & 0xff;
	sendBuf[4] = (src >> 24) & 0xff;
	sendBuf[5] = (src >> 16) & 0xff;
	sendBuf[6] = (src >> 8) & 0xff;
	sendBuf[7] = (src) & 0xff;
}  

/******************/
/* PROCESS PACKET */
/******************/
void processPacket(int currentInv) {
	// Output the buffer
	DEBUG2 dumpbuf(0);
	// short vdc, idc, vac, iac, state, hz, monkwh, lastkwh;
	int cmd, length,i;
	int checksum;
	int expectedLen;
	int responseStart;
	DEBUG2 fprintf(stderr,"ProcessPacket Inv %d Packet len:%d echo %d ", currentInv, data.count, echo);
	if (data.count == 0)
		return;
	if (echo)
		responseStart = data.sentlength;
	else
		responseStart = 0;		
	
	if (test) {
		DEBUG dumpbuf(0);
		length = data.count - responseStart - 17;
		
		DEBUG switch (data.count - responseStart) {
			case 17:
				fprintf(stderr,"\nNo Value\n"); break;
			case 18:
				fprintf(stderr, "\nByte %d\n", data.buf[13 + responseStart]);break;
			case 19:
				fprintf(stderr, "\nWord %d\n", getShort(13 + responseStart));break;
			case 21:
				fprintf(stderr, "\nLong %d (0x%08x)\n", getLong(13 + responseStart),getLong(13 + responseStart));break;
				break;
			default:
				fprintf(stderr, "\nString %d: '", length - 1);
				for (i = 0; i < length - 1; i++)
							 fputc(data.buf[14+responseStart+i], stderr);
				fprintf(stderr, "'\n");
				break;
		}
		return;
	}
	DEBUG2 fprintf(stderr,"Start of response[%d] is 0x%0x ", responseStart, data.buf[responseStart]);
	if (data.count <= responseStart) {
		DEBUG fprintf(stderr, "No data .. discarding as responsStart=%d and data.count=%d\n", responseStart, data.count);
		return;
	}
	if (data.buf[responseStart] != 0x16) {
		DEBUG fprintf(stderr, "Start of data is not 0x16 - discarding\n");
		return;
	}
	checksum = fcs16(INITFCS, &data.buf[3 + responseStart], data.count - responseStart - 7);
	DEBUG2 fprintf(stderr, "Checksum from %d length %d ", 3 + responseStart, data.count - responseStart - 7);
	DEBUG3 fprintf(stderr, "Intermediate FCS = %x (^%x)", checksum, ~checksum & 0xFFFF);
	checksum = fcs16(checksum, &data.buf[data.count - 2], 2);
	if (checksum != GOODFCS) {
		sprintf(buffer, "WARN " PROGNAME " %s Checksum failed got %x instead of %x", 
				unitStr(controllernum, currentInv, useUnits), checksum, GOODFCS);
		logmsg(WARN, buffer);
		return;
	}

	cmd = data.buf[11 + responseStart];
	switch(cmd) {
		case 0x7e: DEBUG fprintf(stderr, "Response 0x%04x OK\n", cmd);	break;
		case 0x7d: DEBUG fprintf(stderr, "Response 0x%04x (Address Set)\n", cmd);	break;
		case 0x72: DEBUG fprintf(stderr, "Response 0x%04x (Data) ", cmd);
			// Only for F2 command 
			expectedLen = responseStart + 17 + sent.length;
			if (expectedLen != data.count) {
				sprintf(buffer, "INFO " PROGNAME " %s Got %d expected %d bytes", unitStr(controllernum, currentInv, useUnits), 
						data.count, expectedLen);
				logmsg(INFO, buffer);
			}
			processData(responseStart + 13, currentInv);
			break;		
		case 0x7A: DEBUG fprintf(stderr, "Response 0x%04x (ACKBUSY) ", cmd);
			sleep(3);
			sendCommand(0, 0, NULL, 0);
			break;
		case 0x73: DEBUG fprintf(stderr, "Response 0x%04x (DataDesc) ", cmd);
			processPart(responseStart + 11);	break;
			
		default: fprintf(stderr, "Response unknown: 0x%04x Ignoring\n", cmd);
	}
}

/***************/
/* PROCESSDATA */
/***************/
void processData(int start, int currentInv) {
	// Work through the sent buf, correlating with the received data.
	// Packet length and checksum has already been checked.
	int offset = start;
	int i, j, code, index, length;
	float val;
	static int state, prevstate[MAXINVERTERS];
	static int event, prevevent[MAXINVERTERS];
	char buf[100], buf2[10];
	char message[100];
	sprintf(message, "INFO " PROGNAME " %s Inverter is %s ", unitStr(controllernum, currentInv, useUnits),
			formatSerial(inverter[currentInv]));
	int gotmessage = 0;
	strcpy(buffer, "inverter ");
	DEBUG dumpbuf(start);
	DEBUG fprintf(stderr, "ProcessData: offset (%d) ", offset);
	for (i = 0; i < sent.num; i++) {
		code = swab(sent.param[i]);
		index = find_code(code);
		if (index == -1) {
			sprintf(buffer, "WARN " PROGNAME " Processing invalid Code %d - ignoring", code);
			logmsg(WARN, buffer);
			return;
		}
		DEBUG2 fprintf(stderr, "\ncode 0x%0x index %d scale %f ", code, index, platbus[index].scale);
		switch(platbus[index].type) {
			case BYTE:
				val = data.buf[offset];	
				DEBUG2 fprintf(stderr, "%d: BYTE %f ", i, val);break;
			case WORD:
				val = getShort(offset);
				DEBUG2 fprintf(stderr, "%d: WORD %f ", i, val);break;
			case LONG:
				val = getLong(offset); 
				DEBUG2 fprintf(stderr, "%d: LONG %f ", i, val);break;
			case STRING:
				gotmessage = 1;
				length = data.buf[offset];
				if (length > 20) {
					sprintf(buffer, "ERROR " PROGNAME " Processing Message length %d - truncating to 20", length);
					logmsg(ERROR, buffer);
					length = 20;
				}
				strncat(message, &data.buf[offset+1], length);
				strcat(message, " ");
				DEBUG2 fprintf (stderr, "Message '%s' ", message);
				break;
				
			default:
				length = platbus[index].length;
				if (length > 20) {
					sprintf(buffer, "ERROR " PROGNAME " Processing String length %d - truncating to 20", length);
					logmsg(ERROR, buffer);
					length = 20;
				}
				DEBUG {
					fprintf(stderr, "\nString %d: '", length);
					for (j = 0; j < length; j++)
						fputc(data.buf[offset + j], stderr);
					fprintf(stderr, "'\n");
				}
				
				val = 0.0;	// Probably a string
		}
		if (platbus[index].scale)
			val /= platbus[index].scale;
		
		// Special case handling
		switch(code) {
			case 0x000b:	state = val;	break;
			case 0x000c:	event = val;	break;
		}
		
		offset += platbus[index].length;
		DEBUG2 fprintf(stderr," adding %d to offset (%d) ", platbus[index].length, offset);
		if (strlen(platbus[index].format)) {
			sprintf(buf2, platbus[index].format, val);
			strcat(buffer, buf2);
			strcat(buffer, " ");
			DEBUG2 fprintf(stderr,"Appending '%s' ", buf2);
		}
	}
			
	if (gotmessage) {
		DEBUG fprintf(stderr, "Message '%s'\n", message);
		logmsg(INFO, message);
	}
	else {
		DEBUG fprintf(stderr, "Inv[%d] '%s'\n", currentInv, buffer);
		sockSend(sockfd[currentInv], buffer);
	}
	// 1.11 Treat 13 as 12
	if (state == 13) state = 12;
	
	if (state != prevstate[currentInv]) 
		stateMessage(prevstate[currentInv] = state, currentInv);
	if (event != prevevent[currentInv]) 
		eventMessage(prevevent[currentInv] = event, currentInv);
}

/****************/
/* PROCESS PART */
/****************/
void processPart(int start) {
	// Like ProcessData but for multipart data
	// First packet: PktNum = 0x8N
	// Subsequent packets 0x0i  i = 1 .. N
	// Checksum and length has already been checked in ProcessData.
	int expectedLen = data.count - start - 6;
	int pktnum;
	cmd = data.buf[start];
	pktnum = data.buf[1 + start];
	DEBUG fprintf(stderr, "ProcessPart pktnum 0x%02x len %d ", pktnum, expectedLen);
	if (pktnum >= 0x80) {	// First packet
		multipart.count = 0;
		multipart.limit = pktnum & 0x7F;
		multipart.ptr = multipart.buf;
	}
	DEBUG2 fprintf(stderr, "Memcpy %02x %02x .. ", data.buf[start + 2], data.buf[start + 3]);
	memcpy(multipart.ptr, data.buf + start+2, expectedLen);
	multipart.ptr += expectedLen;
	multipart.count++;
	if (multipart.count > multipart.limit) {
		displayMultipart();
		cmd = 0xF2;
	}
}

/****************/
/* STATEMESSAGE */
/****************/
void stateMessage(int state, int currentInv) {
	// Write out the statemessage
	// Assume value < 0x1000 are INFO and the rest are WARN
	switch (state) {
		case 0:  sprintf(buffer, "INFO " PROGNAME " %s State (%d) STANDBY", unitStr(controllernum, currentInv, useUnits), state); break;
		case 1:  sprintf(buffer, "INFO " PROGNAME " %s State (%d) Wait DC", unitStr(controllernum, currentInv, useUnits), state); break;
		case 2:  sprintf(buffer, "INFO " PROGNAME " %s State (%d) Wait AC", unitStr(controllernum, currentInv, useUnits), state); break;
		case 10:
		case 11:
		case 12:
		case 13:
		case 14:
		case 16:
		case 29: sprintf(buffer, "INFO " PROGNAME " %s State (%d) Inverting", unitStr(controllernum, currentInv, useUnits), state); break;
		case 30:
		case 31: sprintf(buffer, "INFO " PROGNAME " %s State (%d) Startup test", unitStr(controllernum, currentInv, useUnits), state); break;
		case 100:sprintf(buffer, "WARN " PROGNAME " %s State (%d) ERROR Status", unitStr(controllernum, currentInv, useUnits), state); break;
		case 15: sprintf(buffer, "WARN " PROGNAME " %s State (%d) Derated operation", unitStr(controllernum, currentInv, useUnits), state); break;
		default:
			sprintf(buffer, "WARN " PROGNAME " %s Unknown state: %d", unitStr(controllernum, currentInv, useUnits), state);
	}
	if (strncmp(buffer, "WARN", 4) == 0)
		logmsg(WARN, buffer);
	else
		logmsg(INFO, buffer);
}

/*****************/
/* EVENT MESSAGE */
/*****************/
void eventMessage(int event, int currentInv) {
	// Write out an event message
	int severity = INFO;
	char message[160];
	if (event > 0) severity = WARN;
	if (event >= 400) severity = INFO;
	DEBUG fprintf(stderr, "Enter EventMessage with %d ", event);
	if (event < 100) {
		strcpy(message, "Installation Error (Undocumented)");
	}
	else if (event < 200) {
		strcpy(message, "Installation Error (Undocumented)");
	}
	else if (event < 300) {
		strcpy(message, "Internal System Message (Undocumented)");
	}
	else strcpy(message, "Information (Undocumented)");
	switch (event) {
		case 0:	strcpy(message, "OK");				break;
		case 90: strcpy(message, "MAins voltage (feed phase) exceeds measuring range [AC]");	break;
		case 91: strcpy(message, "DC Voltage too High [DC]");	break;
		case 92: strcpy(message, "DC polarity reversed [DC]");	break;
		case 93: strcpy(message, "Insulation resistance to earth too low [DC]");	break;
		case 94: strcpy(message, "Internal short circuit [Inverter]");	break;
		case 95:
		case 100:
		case 101:
		case 102:
		case 106:
		case 107:
		case 108:
		case 109:
		case 111:
		case 113:
		case 114:
		case 120:
		case 121:
		case 122:
		case 125:
		case 129:
		case 131:
		case 135:
		case 138:
		case 140:
		case 141:
		case 142:
		case 150:
		case 151:
			strcpy(message, "Internal System ERROR [Inverter]");	break;
		case 110: strcpy(message, "Relay error [Inverter]");	break;
		case 112: strcpy(message, "Frequency measurement not possible [AC]");	 break;
		case 127: 
		case 134:
			strcpy(message, "Internal error - set country code [Inverter]"); break;
		case 130: strcpy(message, "L and N reversed [Inverter / AC]");	break;
		case 136: strcpy(message, "Mains type not configured");	break;
		case 200: strcpy(message, "General Error");	break;
		case 201: strcpy(message, "Voltage limit for a single phase out of range [AC]");	break;
		case 202: strcpy(message, "Phase to Phase Voltage Limit out of range for L12 [AC]");	break;
		case 203: strcpy(message, "Phase to Phase Voltage Limit out of range for L23 [AC]");	break;
		case 204: strcpy(message, "Phase to Phase Voltage Limit out of range for L31 [AC]");	break;
		case 208: strcpy(message, "Mains Voltage Surge [AC]");	break;
		case 209: strcpy(message, "Voltage limit for feeding phase exceeded L1 [AC]");	break;
		case 210: strcpy(message, "Frequency L1 exceeded [AC]");	break;
		case 211: strcpy(message, "Frequency L1 under limit [AC]");	break;
		case 212: strcpy(message, "Frequency measurement not plausible [AC]");	break;
		case 213:
		case 214: 
		case 215:
		case 216:
		case 217:
		case 218: 
		case 219:	strcpy(message, "AFI Error [Inverter]");	break;
		case 220:	strcpy (message, "Excess temperature on power semiconductors [Inverter]"); break;
		case 221:
		case 223:	strcpy(message, "Internal temperature too high [Inverter]"); break;
		case 222:	strcpy(message, "Excess temperature on choke coil [Inverter]"); break;
		
		case 224:
		case 225: strcpy(message, "Temperature Error");	break;
		case 226: strcpy(message, "Current limit exceeded [Inverter / AC]"); break;
		case 227:
		case 228:
		case 229:
					strcpy(message, "Internal System error [Inverter]"); break;
		case 230: strcpy(message, "Temperature sensor defect on power semiconductor [Inverter]"); break;
		case 231: strcpy(message, "Temperature sensor defect on choke coil [Inverter]"); break;
		case 232:
		case 233: strcpy(message, "Temperature sensor defect for internal temperature [inverter]"); break;
		case 234:
		case 235: strcpy(message, "Error on relay cut-off [inverter]"); break;
		case 236:
		case 237:
		case 238: strcpy(message, "Error on relay connection [inverter]"); break;
		case 239:
		case 240: strcpy(message, "Error DC measuring AC side offset [Inverter]"); break;
		case 241: strcpy(message, "Excess DC Current [Inverter]"); break;
		case 242: strcpy(message, "Excess DC Voltage [DC]"); break;
		case 245:
		case 246:
		case 253:
		case 247: strcpy(message, "Internal System Error [Inverter]");	break;
		case 250: 
		case 255: strcpy(message, "Internal communication error [Inverter]"); break;
		case 257:
		case 258: strcpy(message, "Communication error with phase balancer [Inverter]"); break;
		case 260: strcpy(message, "RS485 Error");	break;
		case 262: strcpy(message, "Operating Unit Overload");	break;
		case 269: 
		case 270: strcpy(message, "Internal memory error [Inverter]"); break;
		case 271: 
		case 272:
		case 273: strcpy(message, "Internal System Error [Inverter]");	break;
		case 274: strcpy(message, "Voltage from generator too low [DC]");	break;
		case 290: strcpy(message, "Inverter shut down - see other error code"); break;
		case 298: strcpy(message, "Self test failure for specific country code [Inverter]"); break;
		case 300: strcpy(message, "NO feed for too long");	break;
		case 301: strcpy(message, "DC voltage dip");	break;
		case 352: strcpy(message, "Over temperature: derating");	break;
		case 353: strcpy(message, "Comms Error Stringbox");	break;
		case 400: strcpy(message, "Reset [Inverter]");	 break;
		case 402: strcpy(message, "Sunrise [Inverter]");	break;
		case 403: strcpy(message, "Sunset [Inverter]");	break;
		case 409: strcpy(message, "AC outside permissible range [Inverter]");	break;
		case 412:
		case 413: strcpy(message, "Parameters were changed [Inverter]");	break;
		case 417: strcpy(message, "Capacity limit through phase balancer started [Inverter]");	break;
		case 418: strcpy(message, "Capacity limit through phase balancer cancelled [Inverter]");	break;
		case 423: strcpy(message, "Self test complete [Inverter]");	break;
		case 450: strcpy(message, "Reset [Inverter]");	break;
		case 460: 
		case 461: strcpy(message, "Software update performed [Inverter]");	break;
	}
	if (severity == INFO) {
		sprintf(buffer, "INFO " PROGNAME " %s Event (%d) %s", unitStr(controllernum, currentInv, useUnits), event, message);
		logmsg(INFO, buffer);
	}
	else {
		sprintf(buffer, "WARN " PROGNAME " %s Event (%d) %s", unitStr(controllernum, currentInv, useUnits), event, message);
		logmsg(WARN, buffer);
	}		
}

/**********/
/* DETECT */
/**********/
int detect(int commfd, int maxinverters) {
	// The FD 00 xx command asks devices to respond if their address,
	// masked by xx lsb's matches.
	// Ie, FD 00 20 ask for a reply from everyone as address is totally masked
	// FD 00 00 asks for a reply for a perfect match
	// FD 00 10 asks for a reply if the top 16 bits of the address are correct.
	
	// If we can see how two inverters respond, it might be possible to patch this up.
	// Put addresses in inverter[]
	// Return 1 - failure, 0 - success
	
	// For now, run it with detect with one inverter at a time and add stuff to /root/diehl.dat
	
	// Modifed 1.3 - start by discarding buffer.
	// Response of 16 bytes equates to no response at all.
	
	//Modified 1.9 - if useUnits enabled (-U) send a units command if more than maxinverters detected.
	
	fd_set readfd, errorfd;
	struct timeval timeout;
	unsigned int addr = 0;	// Range is 01 to 99.
	// numinverters = 0;	// Set by readfile
	FD_ZERO(&readfd);
	FD_ZERO(&errorfd);
	signed char bit;
	int responseBit;
	// Initial E000 message ...
	
	data.count = 0;
	getbuf(55);
	DEBUG fprintf(stderr, "\nDetect: Initially discarding %d bytes\n", data.count);
	data.count = 0;
	
	setupSendMsg(0xFFFFFFFF, srcaddr);
	sendCommand(0xE0, 0, NULL, 0);
	sendCommand(0xE0, 0, NULL, 0);
	sendCommand(0xE0, 0, NULL, 0);
	while (1) {
		addr = 0;
		for(bit = 32; bit >= 0; bit--) {
			timeout.tv_sec = 1;		// 1 second.  Takes up to 32 seconds to find a complete address
			timeout.tv_usec = 0;
			FD_SET(commfd, &readfd);
			FD_SET(commfd, &errorfd);
			blinkLED(0, REDLED);
			setupSendMsg(addr, srcaddr);
			DEBUG2 fprintf(stderr, "Detecting 0x%x using gap %d .. ", addr, gap);
			sendCommand(0xFD, 0, &bit, 1);
			responseBit = 0;
			
			/* Ugly duplication of code.
			 We are looking for a 0 or 1 response.  In theory no response is a 1, but if
			 running on an interface with RS485 echo, there will always be a response, just
			 a short one that consists only of data.sentlength. 
			 */
			
			if (select(commfd + 1, &readfd, NULL, &errorfd, &timeout) == 0) {	// select timed out.
				// This is the ONE response when there is no echo.
				DEBUG fprintf(stderr, "Bit %d Timeout - treat as 1 ", bit);
				responseBit = 1;
			}
			if (FD_ISSET(commfd, &readfd)) {
				data.count = 0;
				getbuf(200); // Was 55 then 78 - still overflowed
				if (data.count <= data.sentlength + 2 + gap) {	// no response - get pretty much what was sent.
					responseBit = 1;
					DEBUG fprintf(stderr, "Bit %d Short response %d - treat as 1 ", bit, data.count);
				}
				else {
					blinkLED(1, REDLED);
					DEBUG fprintf(stderr, "Bit %d Long response %d - treat as 0 ", bit, data.count);
				}
				blinkLED(0, REDLED);
				if (data.count == 200) fprintf(stderr, "DETECT WARN Possible overflow - got buf 200\n");
			}
			
			if (responseBit) {
				addr |= (1 << bit);
				DEBUG3 fprintf(stderr, "Setting bit %d Addr = %04x ", bit, addr);
			}
			DEBUG fprintf(stderr, " %08x\n", addr);
			
			if (bit == 0 && data.count == 1 && data.buf[0] == STARTSYNC) {		//Not sure about this now
				getbuf(78); // Was 55
				DEBUG fprintf(stderr, "Detect: process final packet\n");
				blinkLED(1, REDLED);
				processPacket(numinverters);		// This normally igves a checksum error
				blinkLED(0, REDLED);
			}
		}
		
		blinkLED(0, REDLED);
		
		/* NOTE weird goings on need careful handling.  
		 Normally the response to FD 00 XX is gibberish or timeout.
		 But response to FD 00 00 is a packet 7D 00, so if we get SYNC Character, need to get rest of packet.
		 */
		
		if (addr != 0xFFFFFFFF) {
			// Add it to inverter[] if not there already
			int i;
			for (i = 0; i < numinverters; i++)
				if (inverter[i] == addr) 
					break;
			
			DEBUG fprintf(stderr, "Processing addr %08x ", addr);		
			if (i < numinverters)  {
				DEBUG fprintf(stderr, "Detected %d - already at inverter[%d]\n", addr, i);
			} else {
				DEBUG fprintf(stderr, "Detected new inverter %d - using inverter[%d]\n", addr, i);
				inverter[numinverters++] = addr;
				if (numinverters == MAXINVERTERS) {
					sprintf(buffer, "ERROR " PROGNAME " %d Exceeded MAXINVERTERS = %d - increase it", 
							controllernum, MAXINVERTERS);
					numinverters--;
				}
			}
			
			// identify(numinverters - 1);
			// THis doesn't work - it sometimes does it for the prevouis inverter if I < numvinverters
			// and always gives a checksum error.
			
			// TODO this is deliberately only code for one inverter.
			// Need more complex logic to detect multiple.
			// The FE00 commands tells an inverter not to respond.
			
			sent.length = 0;
			sent.num = 0;
			setupSendMsg(addr, srcaddr);
			sendCommand(0xFE, 0, NULL, 0);
			DEBUG fprintf(stderr, "FE00 (shutup) to 0x%08x\n", addr);
			data.count = 0;
			synctostart(commfd);
			getbuf(78);	// Was 55
			if (data.count) 
				processPacket(numinverters);
		}
		else break;	// Cos last address found was 0xFFFFFFFF - no more to find.

	}
	
	return numinverters == 0;	// Not found.
}

/************/
/* READFILE */
/************/
void readfile() {
	// Read in the diehlX.dat file
	FILE *f;
	int n;
	char line[120];
	numinverters = 0;
	snprintf(line, sizeof(line), "/root/diehl%d.dat", controllernum);
	if ((f = fopen(line, "r")) == 0) {
		sprintf(buffer, "INFO " PROGNAME " %d No file diehl%d.dat", controllernum, controllernum);
		logmsg(INFO, buffer);
		return;
	}
	while (fgets(line, sizeof(line), f)) {
		DEBUG2 fprintf(stderr, "Line: '%s'", line);
		if (strlen(line) > 0) line[strlen(line) - 1] = 0;	// Remove linefeed
		if (n = readSerial(line)) {
			DEBUG fprintf(stderr, "Reading inverter[%d] as %d (0x%x from %s)\n", numinverters, n, n, line);
			inverter[numinverters++] = n;
			if (numinverters == MAXINVERTERS) {
				sprintf(buffer, "ERROR " PROGNAME " %d Exceeded MAXINVERTERS = %d - increase it", 
						controllernum, MAXINVERTERS);
				logmsg(ERROR, buffer);
				numinverters--;
				fclose(f);
				return;
			};
		}
		else {
			fclose(f);
			return;
		}
	};
	fclose(f);
}
		   
/*************/
/* WRITEFILE */
/*************/
void writefile() {
// Write out the diehlX.dat file
	FILE *f;
	char line[20];
	int i;
	snprintf(line, sizeof(line), "/root/diehl%d.dat", controllernum);
	if ((f = fopen(line, "w")) == 0) {
		sprintf(buffer, "ERROR " PROGNAME " %d Failed to open %s for writing: %s",
				controllernum, line, strerror(errno));
		return;
	}
	for (i = 0; i < numinverters; i++)
		fprintf(f, "%s\n", formatSerial(inverter[i]));
	
	fclose(f);
	DEBUG fprintf(stderr,"Writefile wrote %d lines\n", i);
}

/*************/
/* FIND CODE */
/*************/
int find_code(int code){
	// Return index of CODE within platbus or -1 for not found.
	int i;
	for (i = 0; platbus[i].code; i++)
		if (platbus[i].code == code)
			return i;
	return -1;
}

/*************/
/* FILL SENT */
/*************/
void fillSent(struct platbus * p) {
	int i;
	sent.num = 0;
	sent.length = 0;
	for (i = 0; p->code; i++, p++)
		if (p->inuse) {
			sent.length += p->length;
				sent.param[sent.num++] = swab(p->code);
			DEBUG2 fprintf(stderr,"sent.param[%d]= %04x code=%04x\n",
						   i, sent.param[sent.num-1], p->code);
		}
	DEBUG fprintf(stderr, "FillSent: %d params set, length %d\n", sent.num, sent.length);
}

/*************/
/* READ CONF */
/*************/
void readconf() {
	// Read /root/diehl%d.conf looking for numbers to set in platbus[].
	// If diehl%d.conf not found, try diehl.conf as a fall back.
	FILE *f;
	char name[60];
	int i, n, code;
	DEBUG4 {
		for(i=0; platbus[i].code; i++)
			fprintf(stderr,"Platbus[%d].code = 0x%04x\n", i, platbus[i].code);
	}
		
	snprintf(name, sizeof(name), "/root/diehl%d.conf", controllernum);
	if ((f = fopen(name, "r")) == 0) {
		snprintf(name, sizeof(name), "/root/diehl.conf");
		if ((f = fopen(name, "r")) == 0) {
			sprintf(buffer, "INFO " PROGNAME " %d Configuration file not found, using defaults", controllernum);
			logmsg(INFO, buffer);
			return;
		}
	}
	// Since we are reading a conf file, set all inuse to False
	for (n = 0; platbus[n].code; n++)
		platbus[n].inuse = 0;
	while (fgets(name, sizeof(name), f)) {
		if (n = strtol(name, NULL, 0)) {
			code = find_code(n);
			DEBUG2 fprintf(stderr, "Conf entry %d Code %d (0x%0x)\n", n, code, code);
			if (code != -1) {
				platbus[code].inuse = 1;
				DEBUG fprintf(stderr, "Set platbus[%d].inUse\n", code);
			}
			else {
				sprintf(buffer, "ERROR " PROGNAME " %d Conf entry %d (0x%0x) not valid", 
						controllernum, n, n);
				logmsg(ERROR, buffer);
			}
			
		};
	}
	fclose(f);
}

/************/
/* IDENTIFY */
/************/
void identify(int num) {
	// Do a nice little message about the inverter.
	DEBUG fprintf(stderr, "\nIdentify .. ");
	fillSent(identify_list);
	sendCommand(0xF2, 0, (char *)&sent.param[0], sent.num * 2);	
	data.count = 0;
	getbuf(128);		// Sentlenth = 29 reposnse length = 76
	DEBUG dumpbuf(0);
	processPacket(num);
}

/*********/
/* RESET */
/*********/
void reset(int commfd) {
	// Reset the bus so all members respond.
	int i;
	DEBUG fprintf(stderr, "RESET .. ");
	setupSendMsg(0xFFFFFFFF, srcaddr);
	for (i = 0; i < 3; i++) {
		sendCommand(0xFF, 0, NULL, 0);
		data.count = 0;
		getbuf(78);	// Not expecting any reply
		DEBUG2 fprintf(stderr, "After RESET got %d bytes ", data.count);
		sleep(1);
	}
	data.count = 0;
	DEBUG fprintf(stderr, "Leaving RESET\n");
}

/**********************/
/* DISPLAY MULTI PART */
/**********************/
void displayMultipart() {
	// Print out a multipart Data Desc
	// Can write to file or format as a message.
	char * ptr = multipart.buf;
	int objtype, flags, len, datafmt;
	char strbuf[40];
	char message[100];
	fprintf(stderr, "\nDisplay Description: (buf start = %x end = %x)\n", multipart.buf, multipart.ptr);
	for (ptr = multipart.buf; ptr < multipart.ptr; ptr++)
		fprintf(stderr, "%02x ", *ptr);
	
	ptr = multipart.buf;
	sprintf(message, "INFO " PROGNAME " %s Describe inverter %d Serial %s", unitStr(controllernum, currentInv, useUnits),
			currentInv, formatSerial(inverter[currentInv]));
	logmsg(INFO, message);
	while (ptr < multipart.ptr) {
		sprintf(message, "INFO " PROGNAME " %d DESCRIPTION ", controllernum);
		DEBUG2 fprintf(stderr,"\n[ptr = 0x%0x] ", ptr);
		// Bizarre opimisation error - ptr does not get incremented twice
		//  objtype = ((*ptr++) << 8) | (*ptr++);
		objtype = ((*ptr) << 8) | (ptr[1]);
		ptr+=2;
		DEBUG2 fprintf(stderr,"\n[ptr = 0x%0x] ", ptr);
		DEBUG fprintf(stderr, "ObjID 0x%04x ", objtype);
		sprintf(strbuf, "%04x ", objtype);
		strcat(message, strbuf);
		flags = *ptr++;
		DEBUG2 fprintf(stderr,"\n[ptr = 0x%0x] ", ptr);
	    DEBUG fprintf(stderr, "Flags 0x%02x ", flags);
		datafmt = *ptr++;
		DEBUG fprintf(stderr, "Format 0x%02x ", datafmt);
		len = *ptr++;
		if (len > sizeof(strbuf) - 1) {
			fprintf(stderr, "ERROR len too large: %d\n", len);
			return;
		}
			
		strncpy(strbuf, ptr, len);
		strbuf[len] = 0;
		DEBUG fprintf(stderr, "Name(%d) '%s' ", len, strbuf);
		ptr += len;
		len = *ptr++;
		if (len > sizeof(strbuf) - 1) {
			fprintf(stderr, "ERROR len too large: %d\n", len);
			return;
		}
		strcat(message, strbuf);
		strcat(message, " (");
		strncpy(strbuf, ptr, len);
		strbuf[len] = 0;
		DEBUG fprintf(stderr,"Units(%d) '%s'\n", len, strbuf);
		strcat(message, strbuf);
		strcat(message, ")");
		switch(datafmt & 7) {
		case 1: strcat(message, " /10");	break;
		case 2: strcat(message, " /100");	break;
		case 3: strcat(message, " /1000");	break;
		case 4: strcat(message, " /10000");	break;
		case 7: strcat(message, " *10");	break;
		case 6: strcat(message, " *100");	break;
		case 5: strcat(message, " *1000");	break;
		}
		DEBUG switch(datafmt & 0xf0) {
			case 0: strcat(message, " (char)");	break;
			case 0x10: strcat(message, " (unsigned char)");	break;
			case 0x20: strcat(message, " (short)");	break;
			case 0x30: strcat(message, " (unsigned short)");	break;
			case 0x40: strcat(message, " (long)");	break;
			case 0x50: strcat(message, " (unsigned long)");	break;
			case 0x60: strcat(message, " (long-long)");	break;
			case 0x70: strcat(message, " (unsigned long-long)");	break;
			case 0x80: strcat(message, " (float)");	break;
			case 0x90: strcat(message, " (double)");	break;
			case 0xA0: strcat(message, " (string)");	break;
			case 0xB0: strcat(message, " (time_t)");	break;
			default:   strcat(message, " (???)");
		}
		ptr += len;
		logmsg(INFO, message);
	}
}

/*****************/
/* FORMAT SERIAL */
/*****************/
char * formatSerial(int serial) {
	 // Format a Serial number according to Diehl's method
	static char addr[28];
	sprintf(addr, "%d.%02d.%d.%d.%02d.%02d.%03d",
			(serial >> 28) & 0xf,     // 4 bits
			(serial >> 24) & 0xf,     // 4 bits
			(serial >> 21) & 0x7,     // 3 bits
			(serial >> 16) & 0x1f, // 5 bits
			(serial >> 12) & 0xf,     // 4 bits
			(serial >> 7) & 0x1f,     // 5 bits
			serial & 0x7f);           // 7 bits
	return addr;
}

/***************/
/* READ SERIAL */
/***************/
int readSerial(char * data) {
	// Read a serial number in Diehl format and return it
	int num, s1, s2, s3, s4, s5, s6, s7;
	num = sscanf(data, "%d.%d.%d.%d.%d.%d.%d", &s1, &s2, &s3, &s4, &s5, &s6, &s7);
	DEBUG2 fprintf(stderr,"Readserial converted %d values ", num);
	if (num == 7)
		return (s1 & 0xf) << 28 | (s2 &0xf) << 24 | (s3 & 7) << 21 | (s4 & 0x1f) << 16 | (s5 & 0xf ) << 12
		| (s6 & 0x1f) << 7 | (s7 & 0x7f);
	if (num == 1 && s1 == 0)	// It's probably hex format 0x123435
		return strtol(data, NULL, 0);
	if (num == 1)		// We got one value and it's not zero so assume it's decimal.
		return s1;
	sprintf(buffer, "ERROR " PROGNAME " %d Failed to read inverter number in '%s'", controllernum, data);
	logmsg(ERROR, buffer);
	return 0;
}

/***********/
/* HASECHO */
/***********/
int hasecho(int fd) {
	// return 1 if the port receives what it sends
	int num; 
	data.count = 0;
	
	char message[] = "Hello World";
	// On a very noisy bus this didn't terminate when it was a while() loop.
	for(num = 0; num < 10; num++) {
		getbuf(BUFSIZE);
		data.count = 0;
	}
	write(fd, message, strlen(message));
	num = getbuf(BUFSIZE);
	DEBUG {
		fprintf(stderr,"HasEcho: got %d chars ", data.count);
	dumpbuf(0);
		
	}
	data.count = 0;
	if (num == strlen(message))
		return 1;
	else
		return 0;
}
