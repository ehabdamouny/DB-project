#include "Database.h"
#include <stdlib.h>

using namespace std;

int main(int argc, char* argv[])
{
	Database &db = Database::getInstance();
	int select = 99;
	cout << " Welcome to my Studio. " << endl;
	while (select != 0) {
		cout << "\n\nPlease select an options \n\n"
			<< " 1) Show how many albums recorded between date1 to date2." << endl
			<< " 2) Show how many songs of musician between date1 to date2." << endl
			<< " 3) Show how many albums recorded between date1 to date2 with specific musician." << endl
			<< " 4) Show most popular music instrument." << endl
			<< " 5) Show instruments in selected album." << endl
			<< " 6) Most productive musician between 2 dates." << endl
			<< " 7) Most popular musician between 2 dates." << endl
			<< " 8) How much musicians recorder in all time." << endl
			<< " 9) Musician that assisted more with another musicians." << endl
			<< " 10) Most popular genre over all times." << endl
			<< " 11) The technician that released bigger num of songs." << endl
			<< " 12) First album that was recorded in studio." << endl
			<< " 13) List of songs that includes in 2 or more albums." << endl
			<< " 14) Technicians that recorded whole albums" << endl
			<< " 15) Musician that recorded most songs in different genres" << endl
			<< " 16) End the program." << endl
			<< " Your Choice: ";
		cin >> select;

		switch (select) {

		case 1: {
			db.showBetweenTwoDates();
			cin.clear();
			cin.ignore(10000, '\n');
			break;
		}

		case 2: {
			db.showSongsBetweenTwoDates();
			cin.clear();
			cin.ignore(10000, '\n');
			break;
		}

		case 3: {
			db.showAlbumsBetweenDatesAndComposer();
			cin.clear();
			cin.ignore(10000, '\n');
			break;
		}

		case 4: {
			db.showMostPopularInstrument();
			cin.clear();
			cin.ignore(10000, '\n');
			break;
		}

		case 5: {
			db.instrumentsInAlbum();
			cin.clear();
			cin.ignore(10000, '\n');
			break;
		}

		case 6: {
			db.releaseMostNumOfAlbums();
			cin.clear();
			cin.ignore(10000, '\n');
			break;
		}

		case 7: {
			db.mostPopularManufacturer();
			cin.clear();
			break;
		}

		case 8: {
			db.totalRecordsInMin();
			cin.clear();
			break;
		}

		case 9: {
			db.mostAssistMusician();
			cin.clear();
			break;
		}

		case 10: {
			db.mostPopularGenre();
			cin.clear();
			break;
		}

		case 11: {
			db.bestCustomerDetails();
			cin.clear();
			break;
		}
		case 12: {
			db.firstRecAlbum();
			cin.clear();
			break;
		}

		case 13: {
			db.songsInTwoOrMoreAlbums();
			cin.clear();
			break;
		}

		case 14: {
			db.techniciansInWholeAlbum();
			cin.ignore(10000, '\n');
			break;
		}

		case 15: {
			db.musicianWithMostGenres();
			cin.clear();
			break;
		}
		case 16: {

			cout << "Bye!" << endl;
			exit(0);
		}

		default: {
			cout << "\ninvalid choice" << endl;
			select = 42;
			cin.clear();
			cin.ignore(10000, '\n');
			break;
		}

		}


	}

	return 0;
}