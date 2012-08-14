/*
 * Developed for Yasdi Version yasdi-1.8.0Build11
 *
 * It's going to get a Daemon to log 2 SMA Devices
 *
 * To compile this file:
 * * gcc -o [daemonname] thisfile.c
 * * gcc logger.c -I../../include/ -I../../smalib -I../../libs -o logger -lyasdimaster
 * * gcc logger.c -I../../include/ -I../../smalib -I../../libs -I/usr/include/mysql -L/usr/lib/mysql -o logger -lyasdimaster -lmysqlclient
 *
 * Substitute gcc with cc on some platforms.
 *
 * Christian Schlotter (christian AT cschlotter DOT de)
 * 28/9/2011
 *
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <signal.h>
#include "libyasdimaster.h"
#include <time.h>
#include <mysql.h>

#define DAEMON_NAME "sma-logger"
#define PID_FILE "/var/run/solar-daemon.pid"
#define LOG_FILE "/var/log/devlogger.log"

#define DB_SERVER "localhost"
#define DB_USER "fillusername"
#define DB_PW "fillpassword"
#define DEVICE_COUNT "2"
#define WR_SERNR1 "2100501820"	//wr1
#define WR_SERNR2 "2100513266"	//wr2
#define DB_NAME "solar"

MYSQL * conn;

typedef struct
{
	DWORD DSn;
	DWORD DDHandle;
	DWORD DCHandle;
	int ok;
	int free;
	int timeout;
}Devices;
Devices DDevices[2];

FILE * fp;
int writeLog(char *str)
{
	FILE * fp=fopen(LOG_FILE,"a");
	time_t now = time(0)+7200;
		char buff[20];
		struct tm *sTm;
		sTm = gmtime (&now);
	strftime (buff, sizeof(buff), "%Y-%m-%d %H:%M:%S", sTm);
//	printf(str);
//	printf("\n");
	fprintf(fp, "%s - %s : %s\n", buff, DB_NAME, str);
	fclose(fp);
}

MYSQL * dbConnect(){
	char clogmsg[350];
	MYSQL *conn; MYSQL_RES *res; MYSQL_ROW row;
	char *cserver = DB_SERVER;
	char *cuser = DB_USER;
	char *cpassword = DB_PW;
	char *cdatabase = DB_NAME;
	conn = mysql_init(NULL);
	if (!mysql_real_connect(conn, cserver, cuser, cpassword, cdatabase, 0, NULL, 0)) {
		sprintf(clogmsg, "%s\n", mysql_error(conn));
		writeLog(clogmsg);
		exit(1);
	}else{
		sprintf(clogmsg, "%s\n", mysql_error(conn));writeLog(clogmsg);
	}
	return conn;
}

void dbDisconnect(MYSQL *conn){
	mysql_close(conn);
}

void dbInsertWrData(char * ctablename, int itime, double dvalue) {
	MYSQL * conn;
	conn = dbConnect();
	char clogmsg[350];
	char query_buf[100];
	//dbDisconnect(conn);
//	conn = dbConnect();
	sprintf(query_buf, "INSERT INTO `%s` (`time`, `pac`) VALUES (%i, %lf);", ctablename, itime, dvalue);
	writeLog(query_buf);
//	sprintf(clogmsg, "SQL Query: '%s'", query_buf);
//	writeLog(clogmsg);
//	conn = dbConnect();
	mysql_query(conn, query_buf);
/*	if(!mysql_query(conn, query_buf)) {
*		sprintf(clogmsg, "SQL Query ERROR: %s", mysql_error(conn));
*	        writeLog(clogmsg);
*	}else {
*		sprintf(clogmsg, "SQL Query sollte passen! ERR: %s", mysql_error(conn));
*		writeLog(clogmsg);
*	}
*/
	dbDisconnect(conn);
}

double getChannelSync(DWORD DDeviceHandle, DWORD DChannelHandle){
	char clogmsg[350];
	int ires;
	double dvalue;
	char valuetext[17];
	DWORD valTime;
	ires = GetChannelValue(
		DChannelHandle,    // chan. handle
		DDeviceHandle,  // first device handle
		&dvalue,           // value
		valuetext,        // text value
		16,               // text value size
		8 );              // max. value age

	switch(ires){
		case YE_OK:
//			sprintf(clogmsg, "getChannelSync - YE_OK - PAC: '%lf'", dvalue); writeLog(clogmsg);
			return dvalue;
			break;

//		case YE_UNNOWN_HANDLE:
//			sprintf(clogmsg, "getChannelSync - YE_UNNOWN_HANDLE - 10 Seconds sleep"); writeLog(clogmsg);
//			sleep(10);
//			return -1;
//			break;

		case YE_TIMEOUT:
			sprintf(clogmsg, "getChannelSync - YE_TIMEOUT - 10 Seconds sleep"); writeLog(clogmsg);
			sleep(10);
			return 0;
			break;

		case YE_VALUE_NOT_VALID:
			sprintf(clogmsg, "getChannelSync - YE_VALUE_NOT_VALID - 10 Seconds sleep"); writeLog(clogmsg);
			sleep(10);
			return -1;
			break;

		default:
			sprintf(clogmsg, "getChannelSync - Unknown Error - 10 Seconds sleep"); writeLog(clogmsg);
			sleep(10);
			return -1;
			break;
	}
}

void gWriteChannelSync(DWORD DChannelHandle, DWORD DDeviceHandle, char * ctablename){
	double dvalue;
	DWORD Dtime;
	dvalue = getChannelSync(DChannelHandle, DDeviceHandle);
	Dtime = GetChannelValueTimeStamp(DChannelHandle, DDeviceHandle);
	dbInsertWrData(ctablename, Dtime, dvalue);
}

void startGetChannelAsync( DWORD DChannelHandle, DWORD DDeviceHandle){
	char clogmsg[350];
	int ires;

//	sprintf(clogmsg, "startGetChannelAsync - Started! CHandle: %u , DHandle: %u ", DChannelHandle, DDeviceHandle); writeLog(clogmsg);

	ires = GetChannelValueAsync (
		DChannelHandle,
		DDeviceHandle,
		0
	);
	switch(ires){
		case YE_OK:
//			sprintf(clogmsg, "startGetChannelAsync - YE_OK - Waiting for the Values!"); writeLog(clogmsg);
			break;

		case YE_UNNOWN_HANDLE:
			sprintf(clogmsg, "startGetChannelAsync - YE_UNNOWN_HANDLE"); writeLog(clogmsg);
			break;

		case YE_NO_ACCESS_RIGHTS:
			sprintf(clogmsg, "startGetChannelAsync - YE_NO_ACCESS_RIGHTS"); writeLog(clogmsg);
			break;

		default:
			sprintf(clogmsg, "startGetChannelAsync - Unknown Error"); writeLog(clogmsg);
			break;
		}
	return;
}

void channelCallbackValue( DWORD DChannelHandle, DWORD DDeviceHandle, double dValue, char * cTextValue, int ires ) {
	char clogmsg[350];
	DWORD Dtime;
	int i;
	char ctablename[10];
	int iuse;
	if ( DChannelHandle == DDevices[0].DCHandle )
	{
		iuse = 0;
//		DDevices[0].free = 0;
	}
	else if ( DChannelHandle == DDevices[1].DCHandle )
	{
		iuse = 1;
//		DDevices[1].free = 0;
	}
	DDevices[iuse].free = 0;
	switch(ires)
	{
		case YE_OK:
			Dtime = GetChannelValueTimeStamp(DChannelHandle, DDeviceHandle);
//			if ( iuse == 0 )
//			{
				DDevices[iuse].timeout = 0;
//				ctablename = (char*)DDevices[0].DSn;
				sprintf(ctablename, "%u", DDevices[iuse].DSn);
				dbInsertWrData(ctablename, Dtime, dValue);
/*			}
*			else if ( iuse == 1 )
*			{
*				DDevices[1].timeout = 0;
//				ctablename = (char*)DDevices[1].DSn;
*				sprintf(ctablename, "%u", DDevices[1].DSn);
*				dbInsertWrData(ctablename, Dtime, dValue);
*			}
*/
//			sprintf(clogmsg, "channelCallbackValue - YE_OK - ChannelHandle %d - Value: %lf - Dtime: %d", DChannelHandle ,dValue, Dtime); writeLog(clogmsg);
			break;

		case YE_TIMEOUT:
//			if ( iuse == 0 && DDevices[0].timeout == 0 )
//			{
				sprintf(clogmsg, "channelCallbackValue - YE_TIMEOUT"); writeLog(clogmsg);
				DDevices[iuse].timeout = 1;
/*			}
*			else if ( iuse == 1 && DDevices[0].timeout == 0 )
*			{
*				sprintf(clogmsg, "channelCallbackValue - YE_TIMEOUT"); writeLog(clogmsg);
*				DDevices[1].timeout = 1;
*			}
*/			break;

		default:
			sprintf(clogmsg, "channelCallbackValue - Unknown Error!"); writeLog(clogmsg);
			break;
	}
	return;
}

Devices * detectDevices(){
	char clogmsg[350];
	int iDevCount = 0;
	int iNeededCount = 2;	// Must be Set manually to the sum of devices to use!!
	DWORD DDeviceHandle[iNeededCount];
	int ires;

	int i;
	DWORD Dsn;

	while ( iDevCount < iNeededCount )
	{
		sprintf(clogmsg, "detectDevices - Start an device detection (searching 2 device)"); writeLog(clogmsg);
		ires = DoStartDeviceDetection ( iNeededCount, true );

		switch (ires)
		{
			case YE_OK:
				iDevCount = GetDeviceHandles(DDeviceHandle, 2 );
				sprintf(clogmsg, "detectDevices - Count Devices found: %d . Getting SN", iDevCount); writeLog(clogmsg);
				for ( i = 0;  i < iDevCount; i++ ){
					Dsn;
					ires = GetDeviceSN( DDeviceHandle[i], &Dsn );
					DDevices[i].DDHandle = DDeviceHandle[i];
					switch (ires)
					{
						case YE_OK:
							DDevices[i].DSn = Dsn;
							DDevices[i].ok = 1;
							sprintf(clogmsg, "detectDevices - YE_OK - Device %i SN: %u", iDevCount, Dsn); writeLog(clogmsg);
							break;
						default:
							sprintf(clogmsg, "detectDevices - default - Device %i", iDevCount); writeLog(clogmsg);
					}
				}
				break;

			case YE_NOT_ALL_DEVS_FOUND:
				sprintf(clogmsg, "detectDevices - No / not all Devices found - waiting for 20 seconds and trying again!"); writeLog(clogmsg);
				sleep(20);

			default:
				sprintf(clogmsg, "detectDevices - default - Device %i - waiting for 20 seconds and trying again!", iDevCount); writeLog(clogmsg);
				sleep(20);
		}

	}
	return DDevices;
}

DWORD getChannelHandle(int i){
	char clogmsg[350];
	if( DDevices[i].DCHandle == 0){
			DDevices[i].DCHandle = FindChannelName(DDevices[i].DDHandle, "Pac");
//			sprintf(clogmsg, "current channelHandle[0]: %d", DDevices[i].DCHandle); writeLog(clogmsg);
		}
}

void signal_handler(int sig) {
	char clogmsg[350];
	switch(sig) {
		case SIGHUP:

			sprintf(clogmsg, "signal_handler - Received SIGHUP signal."); writeLog(clogmsg);
			sprintf(clogmsg, "signal_handler - %s is shutting down YASDI", DAEMON_NAME); writeLog(clogmsg);
			yasdiMasterRemEventListener( channelCallbackValue, YASDI_EVENT_CHANNEL_NEW_VALUE );
			yasdiMasterShutdown( );
			exit(EXIT_SUCCESS);
		break;
		case SIGTERM:
			sprintf(clogmsg, "signal_handler - Received SIGTERM signal."); writeLog(clogmsg);
			sprintf(clogmsg, "signal_handler - %s is shutting down YASDI", DAEMON_NAME); writeLog(clogmsg);
			yasdiMasterRemEventListener( channelCallbackValue, YASDI_EVENT_CHANNEL_NEW_VALUE );
			yasdiMasterShutdown( );
			exit(EXIT_SUCCESS);
		break;
		default:
			sprintf(clogmsg, "signal_handler - Unhandled signal "); writeLog(clogmsg);
			sprintf(clogmsg, "signal_handler - %s is shutting down YASDI", DAEMON_NAME); writeLog(clogmsg);
			yasdiMasterRemEventListener( channelCallbackValue, YASDI_EVENT_CHANNEL_NEW_VALUE );
			exit(EXIT_SUCCESS);
			yasdiMasterShutdown( );
		break;
	}
}

int main(int argc, char *argv[]) {

//	conn = dbConnect();
	char clogmsg[350];
	signal(SIGHUP, signal_handler);
	signal(SIGTERM, signal_handler);
	signal(SIGINT, signal_handler);
	signal(SIGQUIT, signal_handler);

	sprintf(clogmsg, "main - %s daemon starting up", DAEMON_NAME); writeLog(clogmsg);

	pid_t pid, sid;
	pid = fork();
	if (pid < 0) {
		exit(EXIT_FAILURE);
	}
	if (pid > 0) {
		exit(EXIT_SUCCESS);
	}
	umask(0);
	sid = setsid();
	if (sid < 0) {
		exit(EXIT_FAILURE);
	}
	if ((chdir("/")) < 0) {
		exit(EXIT_FAILURE);
	}
	close(STDIN_FILENO);
	close(STDOUT_FILENO);
	close(STDERR_FILENO);

/*
 *	END OF DAEMON STARTUP
 *	------------------------------
 *	DAEMON-SPECIFIC INITIALIZATION
 *
 *	Max 2 Channels, Max 2 Devices
 *
 */

	DWORD dwBDC=0;
	DWORD DChannelHandle[2] = {0};
	char cChanName[]="Pac";
	int ires;
	int i;

	sprintf(clogmsg, "main - Init yasdi master"); writeLog(clogmsg);
	yasdiMasterInitialize("/etc/yasdi/yasdi.ini",&dwBDC);

	sprintf(clogmsg, "main - Set all available YASDI bus drivers online"); writeLog(clogmsg);
	for ( i=0; i < dwBDC; i++)
	{
		yasdiMasterSetDriverOnline( i );
	}
	if ( 0==dwBDC)
	{
		sprintf(clogmsg, "main - Warning: No YASDI driver was found."); writeLog(clogmsg);
	}
/*
 *	END OF DAEMON-SPECIFIC INITIALIZATION
 *	-------------------------------------
 *	THE DAEMON
 */

	yasdiMasterAddEventListener( channelCallbackValue, YASDI_EVENT_CHANNEL_NEW_VALUE );

	detectDevices();
	for ( i=0; i < atoi(DEVICE_COUNT); i++)
	{
		getChannelHandle(i);
		DDevices[i].free = 0;
		DDevices[i].timeout = 0;
	}
	i = 0;
	while (1) {
		if ( DDevices[0].free == 0)
		{
			sleep(1);
			startGetChannelAsync( DDevices[0].DCHandle, DDevices[0].DDHandle );
			DDevices[0].free = 1;
		}
		sleep(3);
		if ( DDevices[1].free == 0)
		{
			sleep(1);
			startGetChannelAsync( DDevices[1].DCHandle, DDevices[1].DDHandle );
			DDevices[1].free = 1;
		}
		sleep(3);
	}

	sprintf(clogmsg, "main - %s is shutting down YASDI", DAEMON_NAME); writeLog(clogmsg);
	yasdiMasterRemEventListener( channelCallbackValue, YASDI_EVENT_CHANNEL_NEW_VALUE );
	yasdiMasterShutdown( );

	// *******************************
	// *** END OF LOGGER
	sprintf(clogmsg, "main - %s daemon exiting", DAEMON_NAME); writeLog(clogmsg);
	exit(EXIT_SUCCESS);
//	dbDisconnect(conn);
}

