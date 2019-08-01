#ifndef _DB_H
#define _DB_H

#include "mysql_connection.h"

#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>
#include <cppconn/prepared_statement.h>
#include <iostream>

using namespace sql;
using namespace std;
#define DB_NAME "studio"
#define DB_HOST "tcp://127.0.0.1/"
#define DB_PORT 3306
#define DB_USER "root"
#define DB_PASS "root"

class Database {
private:
	// singleton instance
	static Database *instance;

	Driver *driver;
	ConnectOptionsMap connection_properties;

	// private CTOR (singleton)s
	Database();
public:
	static Database& getInstance();

	Connection *getConnection();

	virtual ~Database() {}


	/* Menu Options Functions */

	//Option 1
	void showBetweenTwoDates();
	//Option 2
	void showSongsBetweenTwoDates();
	//Option 3
	void showAlbumsBetweenDatesAndComposer();
	//Option 4
	void showMostPopularInstrument();
	//Option 5
	void instrumentsInAlbum();
	//Option 6
	void releaseMostNumOfAlbums();
	//Option 7
	void mostPopularManufacturer();
	// Option 8
	void totalRecordsInMin();
	// Option 9
	void mostAssistMusician();
	// Option 10
	void mostPopularGenre();
	// Option 11 
	void bestCustomerDetails();
	//Option 12
	void firstRecAlbum();
	//Option 13
	void songsInTwoOrMoreAlbums();
	//Option 14
	void techniciansInWholeAlbum();
	//Option 15
	void musicianWithMostGenres();



};
#endif