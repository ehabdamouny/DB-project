#include "Database.h"
//#include <iostream>
#include <exception>
using namespace std;
void clearCin() {
	string temp;
	getline(cin, temp);
}


Database *Database::instance = 0;

Database::Database() : driver(get_driver_instance()) {
	connection_properties["hostName"] = DB_HOST;
	connection_properties["port"] = DB_PORT;
	connection_properties["userName"] = DB_USER;
	connection_properties["password"] = DB_PASS;
	connection_properties["OPT_RECONNECT"] = true;
}


Database & Database::getInstance() {
	if (Database::instance == 0) {
		instance = new Database();
	}
	return *instance;
}

Connection * Database::getConnection() {
	try {
		Connection *con = driver->connect(connection_properties);
		con->setSchema(DB_NAME);
		return con;
	}
	catch (SQLException &e) {
		cout << e.what();
	}
	return 0;
}


//Option 1
void Database::showBetweenTwoDates() {
	string temp1, temp2;
	Connection* con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	ResultSet* rset;
	PreparedStatement* pstmt = con->prepareStatement("SELECT * FROM albums WHERE st_date >=? AND fin_date <= ?;");

	cout << endl << "Please enter date in this pattern YYYY-MM-DD\n" << endl;
	cout << "Please enter from date: ";
	clearCin();
	getline(cin, temp1);
	pstmt->setString(1, temp1);
	cout << "Please enter to date: ";
	getline(cin, temp2);
	pstmt->setString(2, temp2);
	rset = pstmt->executeQuery();

	if (rset->first()) {
		cout << "Number of recorded albums between " << temp1 << " and " << temp2 << " is: " << rset->rowsCount() << "." << endl;
	}
	else cout << "Invalid Date or No albums That Where recorded Between Dates." << endl;


	delete con;
	delete pstmt;
	delete rset;
}

//Option 2
void Database::showSongsBetweenTwoDates() {

	string date1, date2, mus;
	Connection* con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	ResultSet* rset;
	PreparedStatement* pstmt = con->prepareStatement("select * from participate left join tracks on tracks.song_id = participate.song_id left join musicians on musicians.mus_id = participate.mus_id where musicians.fname = ? and  ? < tracks.date  and tracks.date < ? ");


	cout << endl << "Please enter date in this pattern YYYY-MM-DD\n" << endl;
	cout << "Please enter from date: ";
	clearCin();
	getline(cin, date1);
	pstmt->setString(2, date1);
	cout << "Please enter to date: ";
	getline(cin, date2);
	pstmt->setString(3, date2);
	cout << "Please enter musician Name: ";
	getline(cin, mus);
	pstmt->setString(1, mus);

	rset = pstmt->executeQuery();

	if (rset->first()) {
		cout << "Number of recorded songs of " << mus << " between " << date1 << " and " << date2 << " is: " << rset->rowsCount() << "." << endl;
	}
	else cout << "Invalid Date or No Orders Found That Where Ordered Between Dates." << endl;


	delete con;
	delete pstmt;
	delete rset;
}

//Option 3
void Database::showAlbumsBetweenDatesAndComposer() {
	string date1, date2, mus;
	Connection* con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	ResultSet* rset;
	PreparedStatement* pstmt = con->prepareStatement("select * from participate  left join tracks on tracks.song_id = participate.song_id  left join musicians on musicians.mus_id = participate.mus_id left join contains on tracks.song_id = contains.song_id left join albums on contains.album_id = albums.album_id where fname = ? and st_date > ? and fin_date < ? group by contains.album_id");
	cout << endl << "Please enter date in this pattern YYYY-MM-DD\n" << endl;
	cout << "Please enter from date: ";
	clearCin();
	getline(cin, date1);
	pstmt->setString(2, date1);
	cout << "Please enter to date: ";
	getline(cin, date2);
	pstmt->setString(3, date2);
	cout << "Please enter musician Name: ";
	getline(cin, mus);
	pstmt->setString(1, mus);

	rset = pstmt->executeQuery();

	if (rset->first()) {
		cout << "Number of recorded albums where included " << mus << " between " << date1 << " and " << date2 << " is: " << rset->rowsCount() << "." << endl;
	}
	else cout << "Invalid Date or No Orders Found That Where Ordered Between Dates." << endl;


	delete con;
	delete pstmt;
	delete rset;
}

//Option 4
void Database::showMostPopularInstrument() {
	Connection *con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	Statement *stmt = con->createStatement();
	ResultSet* rset = stmt->executeQuery("select instruments.name,count(instruments.inst_id) as count from participate  left join tracks on tracks.song_id = participate.song_id  left join musicians on musicians.mus_id = participate.mus_id left join contains on tracks.song_id = contains.song_id left join albums on contains.album_id = albums.album_id left join play_on on play_on.mus_id = musicians.mus_id left join instruments on play_on.inst_id = instruments.inst_id group by instruments.inst_id order by count(instruments.inst_id) DESC limit 1; ");

	rset->beforeFirst();

	if (rset->first()) {
		cout << "Most used instrument is: " << rset->getString("name") << " appears " << rset->getString("count") << " times." << endl;
	}

	delete con;
	delete stmt;
	delete rset;
}

//Option 5
void Database::instrumentsInAlbum() {
	string album;
	int counter = 1;
	Connection* con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	ResultSet* rset;
	PreparedStatement* pstmt = con->prepareStatement("select * from participate  left join tracks on tracks.song_id = participate.song_id  left join musicians on musicians.mus_id = participate.mus_id left join contains on tracks.song_id = contains.song_id left join albums on contains.album_id = albums.album_id left join play_on on play_on.mus_id = musicians.mus_id left join instruments on play_on.inst_id = instruments.inst_id where albums.name = ? group by instruments.name;");

	cout << "Please enter album name: ";
	clearCin();
	getline(cin, album);
	pstmt->setString(1, album);

	rset = pstmt->executeQuery();

	if (rset->rowsCount() == 0) {
		cout << "No such album found" << endl;
		return;
	}

	rset->beforeFirst();
	cout << "Instruments that album " << album << " includes are:" << endl;

	while (rset->next()) {
		cout << counter << ") " << rset->getString("name") << endl;
		++counter;
	}

	delete con;
	delete pstmt;
	delete rset;
}

//Option 6
void Database::releaseMostNumOfAlbums() {
	string date1, date2;
	Connection* con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	ResultSet* rset;
	PreparedStatement* pstmt = con->prepareStatement("select producers.name as names,count(producers.prod_id) as count from albums left join producers on albums.prod_id = producers.prod_id where albums.st_date > ? and ? < albums.fin_date group by producers.prod_id order by producers.prod_id;");

	cout << endl << "Please enter date in this pattern YYYY-MM-DD\n" << endl;
	cout << "Please enter from date: ";
	clearCin();
	getline(cin, date1);
	pstmt->setString(1, date1);
	cout << "Please enter to date: ";
	getline(cin, date2);
	pstmt->setString(2, date2);
	rset = pstmt->executeQuery();

	if (rset->first()) {
		cout << "Most released albums producer is: " << rset->getString("names") << " which released " << rset->getString("count") << " albums." << endl;
	}
	else cout << "Invalid Date or No Orders Found That Where Ordered Between Dates." << endl;

	delete con;
	delete pstmt;
	delete rset;
}

//Option 7
void Database::mostPopularManufacturer() {
	Connection* con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	Statement* stmt = con->createStatement();
	ResultSet* rset = stmt->executeQuery("select musicians.fname as result, count(instruments.inst_id) as count  from musicians left join participate on musicians.mus_id = participate.mus_id left join tracks on participate.song_id = tracks.song_id left join play_on on musicians.mus_id = play_on.mus_id left join instruments on instruments.inst_id = play_on.inst_id group by musicians.mus_id order by musicians.mus_id;");


	if (rset->first()) {
		cout << "Most popular manufacturer maker is: " << rset->getString("result") << "and his instruments take part in " << rset->getString("count") << " records." << endl;
	}

	delete con;
	delete stmt;
	delete rset;
}

// Option 8
void Database::totalRecordsInMin() {

	Connection* con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	Statement* stmt = con->createStatement();
	ResultSet* rset = stmt->executeQuery("select * from participate left join musicians on musicians.mus_id = participate.mus_id left join tracks on participate.song_id = tracks.song_id group by musicians.mus_id order by musicians.mus_id;");
	rset->beforeFirst();

	if (rset->first()) {
		cout << "the total number of musicians are ever recorded is : " << rset->rowsCount() << " musicians" << endl;
	}

	delete con;
	delete stmt;
	delete rset;
}

// Option 9
void Database::mostAssistMusician() {

	Connection* con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	Statement* stmt = con->createStatement();
	ResultSet* rset = stmt->executeQuery("select fname from  participate left join musicians on participate.mus_id = musicians.mus_id left join tracks on tracks.song_id = participate.song_id  group by participate.mus_id having count(participate.song_id) order by count(participate.song_id) DESC limit 1;");
	rset->beforeFirst();

	if (rset->first()) {
		cout << "Most assisted musician : " << rset->getString("fname") << endl;
	}

	delete con;
	delete stmt;
	delete rset;
}

// Option 10
void Database::mostPopularGenre() {
	Connection* con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	Statement* stmt = con->createStatement();
	ResultSet* rset = stmt->executeQuery("select genre,count(genre) as count from tracks group by genre order by count DESC limit 1;");
	rset->beforeFirst();

	if (rset->first()) {
		cout << "Most popular genre : " << rset->getString("genre") << " with " << rset->getString("count") << " songs" << endl;
	}

	delete con;
	delete stmt;
	delete rset;
}

// Option 11 
void Database::bestCustomerDetails() {

	string date1, date2;
	Connection *con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	ResultSet *rset;
	PreparedStatement *pstmt = con->prepareStatement("select technicians.name as result,count(tracks.tech_id) as count from tracks  left join technicians on tracks.tech_id = technicians.tech_id where tracks.date > ? and tracks.date < ? group by tracks.tech_id order by count DESC limit 1");
	cout << "\nPlease enter date in this pattern YYYY-MM-DD\n" << endl;
	cout << "Please Enter from Date:> ";
	clearCin();
	getline(cin, date1);
	pstmt->setString(1, date1);
	cout << "Please Enter to Date:> ";
	getline(cin, date2);
	pstmt->setString(2, date2);
	rset = pstmt->executeQuery();

	if (rset->first()) {
		cout << "the techinician who recorded most of the songs is: " << rset->getString("result") << " " << ", and he recorded " << rset->getUInt("count") << "tracks " << "from:" << date1 << " to: " << date2 << endl;
	}

	else cout << "Invalid Date or No tracks who recorded in those dates." << endl;


	delete con;
	delete pstmt;
	delete rset;
}

//Option 12
void Database::firstRecAlbum() {
	Connection* con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	Statement* stmt = con->createStatement();
	ResultSet* rset = stmt->executeQuery("select name,fin_date from albums order by fin_date limit 1;");
	rset->beforeFirst();

	if (rset->first()) {
		cout << "First recorded album is: " << rset->getString("name") << " at " << rset->getString("fin_date") << endl;
	}

	delete con;
	delete stmt;
	delete rset;
}

//Option 13
void Database::songsInTwoOrMoreAlbums() {

	int counter = 1;
	Connection* con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	Statement* stmt = con->createStatement();
	ResultSet* rset = stmt->executeQuery("select tracks.name as name,count(contains.song_id) as count from contains left join albums on contains.album_id = albums.album_id left join tracks on contains.song_id = tracks.song_id group by contains.song_id HAVING count > 1;");

	rset->beforeFirst();

	if (rset->first()) {
		cout << "The songs who includes in two or more albums are: " << endl;
		do {
			cout << counter << ")" << rset->getString("name") << " includes in: " << rset->getString("count") << " albums." << endl;
			++counter;
		} while (rset->next());
	}
	else cout << "No duplicate songs found." << endl;

	delete con;
	delete stmt;
	delete rset;
}

//Option 14
void Database::techniciansInWholeAlbum() {

	int counter = 1;
	Connection* con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	Statement* stmt = con->createStatement();
	ResultSet* rset = stmt->executeQuery("select technicians.name as result from contains left join albums on contains.album_id = albums.album_id  left join tracks on contains.song_id = tracks.song_id  left join technicians on technicians.tech_id = tracks.tech_id group by albums.album_id having count(tracks.tech_id)=1;");
	rset->beforeFirst();

	if (rset->first()) {
		cout << "the technicians who recorded whole album are: " << endl;
		while (rset->next()) {
			cout << counter << ") " << rset->getString("name") << endl;
			++counter;
		}
	}

	delete con;
	delete stmt;
	delete rset;
}

//Option 15
void Database::musicianWithMostGenres() {

	Connection* con = driver->connect(connection_properties);
	con->setSchema(DB_NAME);
	Statement* stmt = con->createStatement();
	ResultSet* rset = stmt->executeQuery("select fname result ,count(distinct genre) as count from participate left join musicians on participate.mus_id = musicians.mus_id left join tracks on tracks.song_id = participate.song_id group by fname order by count(genre) DESC;");
	rset->beforeFirst();

	if (rset->first()) {
		cout << "Most genres musician: " << rset->getString("result") << " with " << rset->getString("count") << " genres." << endl;
	}

	delete con;
	delete stmt;
	delete rset;

}