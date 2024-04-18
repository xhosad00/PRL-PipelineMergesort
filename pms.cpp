/**
 * @file    pms.cpp
 * @brief   PRL project #1
 * @author  David Drtil <xdrtil03@stud.fit.vutbr.cz>
 * @date    2024-04-01
 * @details Implementation of Parallel Merge Sort algorithm using openMPI,
 *          capable of sorting upto 2048 numbers,
 *          due to the limitation of max. 12 processors.
 */

#include <iostream>
#include <fstream>
#include <queue>
#include <math.h>
#include <climits>
#include <mpi.h>

#define MY_TAG 1
#define STOP_SIGNAL -1  //todo INT_MAX  // For signaling end of recieving

void readNumbersAndPropagate(int procId, int procCnt) {
    std::ifstream file("numbers", std::ios::in);
    if (file.is_open()) {
        int number;     // Temporary variable for storing numbers
        if (procCnt == 1) {
            while ((number = file.get()) != EOF) {
                std::cout << number << " " << std::endl;
                std::cout << number << " " << std::endl;
            }
        }
        else {
            while ((number = file.get()) != EOF) {
                std::cout << number << " ";     // Print unordered sequence of numbers, 1 by 1
                MPI_Send(&number, 1, MPI_INT, procId + 1, MY_TAG, MPI_COMM_WORLD); // Sending number to 2nd
            }

            // End of file, sending stop signal to other processors
            int recv = procId + 1;      // Receiver is next processor
            int stop_sig = STOP_SIGNAL;
            MPI_Send(&stop_sig, 1, MPI_INT, recv, MY_TAG, MPI_COMM_WORLD);
            // std::cout << std::endl;
        }
        file.close();
    }
    else {
        std::cerr << "Error: Failed to open file \"numbers\"." << std::endl;
        MPI_Abort(MPI_COMM_WORLD, -1);
    }
}

// void sendNumbersToNext() {

// }

void printQueueData(std::queue<int> q) {
    std::queue<int> tempQueue = q;
    // std::cout << " elems: ";
    while (!tempQueue.empty()) {
        std::cout << "[" << tempQueue.front() << "] "; // Print the front element
        tempQueue.pop(); // Remove the front element
    }
    std::cout << std::endl;
}

void sendNumberToNextProcessor(int procId, bool& sendingEnabled, int& sentFromLine1, int& sentFromLine2, int lineLength, std::queue<int>& line1, std::queue<int>& line2) {
    if (sendingEnabled) {
        // Sending number to next processor
		//std::cout << "[" << procId << "] sending "; // << sentFromLine1 << " " << sentFromLine2 << " "<< line1.size() << " "<< line2.size() << "\n";
        int reciever = procId + 1;
			std::cout << " [" << procId << "] sending " << sentFromLine1 << " " << sentFromLine2 << " "<< line1.size() << " "<< line2.size() << " "<< lineLength<< "\n";
        if (sentFromLine1 != lineLength && sentFromLine2 != lineLength) {
            // Both lines can send numbers to next processor
            if (!line1.empty() && !line2.empty()) {
                if (line1.front() < line2.front()) {
                    MPI_Send(&line1.front(), 1, MPI_INT, reciever, MY_TAG, MPI_COMM_WORLD);
                    line1.pop();
                    sentFromLine1++;
					std::cout << " [" << procId << "] sending " << sentFromLine1 << " " << sentFromLine2 << " "<< line1.size() << " "<< line2.size() << "\n";
                }
                else {
                    MPI_Send(&line2.front(), 1, MPI_INT, reciever, MY_TAG, MPI_COMM_WORLD);
                    line2.pop();
                    sentFromLine2++;
                }
            }
            else if (!line1.empty() && line2.empty()) {  // todo zkusit vymazat
                MPI_Send(&line1.front(), 1, MPI_INT, reciever, MY_TAG, MPI_COMM_WORLD);
                line1.pop();
                sentFromLine1++;
            }
            else if (line1.empty() && !line2.empty()) {     // todo zkusit vymazat
            // else {   // todo
                MPI_Send(&line2.front(), 1, MPI_INT, reciever, MY_TAG, MPI_COMM_WORLD);
                line2.pop();
                sentFromLine2++;
            }
        }
        else if (sentFromLine1 == lineLength && sentFromLine2 != lineLength) {
            MPI_Send(&line2.front(), 1, MPI_INT, reciever, MY_TAG, MPI_COMM_WORLD);
            line2.pop();
            sentFromLine2++;
        }
        else if (sentFromLine1 != lineLength && sentFromLine2 == lineLength) {
            MPI_Send(&line1.front(), 1, MPI_INT, reciever, MY_TAG, MPI_COMM_WORLD);
            line1.pop();
            sentFromLine1++;
        }
        if (sentFromLine1 == lineLength && sentFromLine2 == lineLength) {
            sendingEnabled = false;
            sentFromLine1 = 0;
            sentFromLine2 = 0;
        }
    }

    // todo toto potom obnovit
    // else if ((line1.empty() && line2.size() == 1)
    //     || (line2.empty() && line1.size() == 1)) {
    //     // Send stop signal to next processor, last number was sent
    //     int stop_sig = STOP_SIGNAL;
    //     MPI_Send(&stop_sig, 1, MPI_INT, procId+1, MY_TAG, MPI_COMM_WORLD);
    // }
}

void printRemainingLines(bool sendingEnabled, std::queue<int>& line1, std::queue<int>& line2) {
    if (sendingEnabled) {
        if (!line1.empty() && !line2.empty()) {
            if (line1.front() < line2.front()) {
                std::cout << line1.front() << std::endl;
                line1.pop();
            }
            else {
                std::cout << line2.front() << std::endl;
                line2.pop();
            }
        }
        else if (!line1.empty() && line2.empty()) {
            std::cout << line1.front() << std::endl;
            line1.pop();
        }
        else {
            std::cout << line2.front() << std::endl;
            line2.pop();
        }
    }
}

void recieveNumbersFromPrev(int procId, int procCnt) {
	
	std::cout << "ProcId[" << procId << "] Line1_size: \n";
    std::queue<int> line1 = {};
    std::queue<int> line2 = {};

    int lineLength = pow(2, procId-1);
    int sentFromLine1 = 0;
    int sentFromLine2 = 0;
    int recvToLine1 = 0;
    int recvToLine2 = 0;
    bool recievingEnabled = true;
    bool sendingEnabled = false;
    int number;
    int sender = procId - 1;    // Sender was previous processor

    // while (recievingEnabled || !line1.empty() || !line2.empty()) {  // todo
	int t = 0;
    while (recievingEnabled || !(line1.empty() && line2.empty())) {
        if (recievingEnabled) {
            MPI_Status status;
            MPI_Recv(&number, 1, MPI_INT, sender, MY_TAG, MPI_COMM_WORLD, &status);
            if (number == STOP_SIGNAL) {
                recievingEnabled = false;
            }
            else {
                if (recvToLine1 < lineLength) {
                    line1.push(number);
                    recvToLine1++;
                }
                else if (recvToLine2 < lineLength) {
                    line2.push(number);
                    recvToLine2++;
                }
                if (recvToLine1 == lineLength && recvToLine2 == 1) {
                    sendingEnabled = true;
                }
                if (recvToLine1 == lineLength && recvToLine2 == lineLength) {
                    recvToLine1 = 0;
                    recvToLine2 = 0;
                }
            }
        }

        // std::cout << "ProcId[" << procId << "] Line1_size: " << line1.size() << " ";
        // printQueueData(line1);
        // std::cout << "ProcId[" << procId << "] Line2_size: " << line2.size() << " ";
        // printQueueData(line2);

        bool notLast = procId != procCnt - 1;
        if (notLast) {
            // Middle processor
			t++;
			if (t >5)
				break;
			std::cout << "[" << procId << "] " << sentFromLine1 << " " << sentFromLine2 << " "<< line1.size() << " "<< line2.size() << " "<< lineLength<< "\n";
            sendNumberToNextProcessor(procId, sendingEnabled, sentFromLine1, sentFromLine2, lineLength, line1, line2);
        }
        else {
            // Last processor
            // printRemainingLines(sendingEnabled, line1, line2);
        }

        // std::cout << "ProcId[" << procId << "] Line1_size: " << line1.size() << " ";
        // printQueueData(line1);
        // std::cout << "ProcId[" << procId << "] Line2_size: " << line2.size() << " ";
        // printQueueData(line2);
    }

    // todo toto smazat!
    if (procId != procCnt - 1) {
        int stop_sig = STOP_SIGNAL;
        MPI_Send(&stop_sig, 1, MPI_INT, procId+1, MY_TAG, MPI_COMM_WORLD);
    }
}

int main(int argc, char **argv){
    int procId, procCnt;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &procId);     // Get the rank of the current processor
    MPI_Comm_size(MPI_COMM_WORLD, &procCnt);    // Get the processors count

    if (procId == 0) {
        // 1st processor reads numbers from input file and sends them to 2nd processor
        readNumbersAndPropagate(procId, procCnt);
    }
    else {
        // Other processors perform sorting
    	// if (procId == 0) {
		if (procId == 1)
        	recieveNumbersFromPrev(procId, procCnt);
    }
    // todo
    // else if (procId != procCnt - 1) {
    //     // Other processors perform sorting
    //     sortNumbers(procId, procCnt);
    // }
    // else {
    //     // Last processor receives sorted numbers
    //     receiveSortedNumbers(procId, procCnt);
    // }

    MPI_Finalize();
    return 0;
}
