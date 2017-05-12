#include "mpi.h"
#include "stdlib.h"
#include "time.h"
#include "stdio.h"

void Divide(int* arrayToDivide, int** downArray, int** upArray, int sizeOfArrayToDivide, int* sizeOfDownArray, int* sizeOfUpArray, int pivot){

   int i = 0;

   *sizeOfDownArray = 0;

   for(i = 0; i < sizeOfArrayToDivide; ++i){
      if(arrayToDivide[i] <= pivot){
         ++(*sizeOfDownArray);
      }
   }

   *sizeOfUpArray = sizeOfArrayToDivide - *sizeOfDownArray;

   *downArray = (int*)malloc(*(sizeOfDownArray) * sizeof(int));
   *upArray = (int*)malloc(*(sizeOfUpArray) * sizeof(int));

   int downArrayInd = 0, upArrayInd = 0;

   for(i = 0; i < sizeOfArrayToDivide; ++i){
      if(arrayToDivide[i] <= pivot){
         (*downArray)[downArrayInd] = arrayToDivide[i];
         ++downArrayInd;
      } else {
           (*upArray)[upArrayInd] = arrayToDivide[i];
           ++upArrayInd;
      }
   }
}

int* Merge(int* sortedArray1, int* sortedArray2, int sortedArray1Length, int sortedArray2Length){

   int* sortedArray = (int*)malloc((sortedArray1Length + sortedArray2Length) * sizeof(int));
   int i = 0, sortedArray1Ind = 0, sortedArray2Ind = 0;

   for (i = 0; i < sortedArray1Length + sortedArray2Length; ++i) {
      if ((sortedArray1Ind < sortedArray1Length) && (sortedArray2Ind < sortedArray2Length)){
         if (sortedArray1[sortedArray1Ind] < sortedArray2[sortedArray2Ind]){
            sortedArray[i] = sortedArray1[sortedArray1Ind];
            sortedArray1Ind++;
         } else {
              sortedArray[i] = sortedArray2[sortedArray2Ind];
              sortedArray2Ind++;
           }
      } else if (sortedArray1Ind < sortedArray1Length) {
           sortedArray[i] = sortedArray1[sortedArray1Ind];
           sortedArray1Ind++;
        } else {
             sortedArray[i] = sortedArray2[sortedArray2Ind];
             sortedArray2Ind++;
          }
   }
   return sortedArray;
}

void QuickSort(int* arrayToSort, int leftBound, int rightBound) {

   if (leftBound >= rightBound) {
      return;
   }

   int leftInd = leftBound, rightInd = rightBound, buf = 0;

   int pivotInd = leftBound, pivot = arrayToSort[pivotInd];

   while (1) {

      while ((arrayToSort[leftInd] <= pivot) && (leftInd < rightBound)) {
         ++leftInd;
      }

      while ((arrayToSort[rightInd] >= pivot) && (rightInd > leftBound)) {
         --rightInd;
      }

      if (leftInd >= rightInd) {
         break;
      }

      buf = arrayToSort[leftInd];
      arrayToSort[leftInd] = arrayToSort[rightInd];
      arrayToSort[rightInd] = buf;
    }

    buf = arrayToSort[pivotInd];
    arrayToSort[pivotInd] = arrayToSort[rightInd];
    arrayToSort[rightInd] = buf;

    QuickSort(arrayToSort, leftBound, rightInd - 1);
    QuickSort(arrayToSort, rightInd + 1, rightBound);
}

int main(int argc, char *argv[]){

   int size = 100000000;
   srand(time(NULL));

   int procRank = 0, procNum = 0, nameLen = 0, i = 0, j = 0, k = 0;
   char procName[MPI_MAX_PROCESSOR_NAME];
   double startTime = 0, endTime = 0;

   int* arrayToSort;

   MPI_Init(&argc, &argv);
   MPI_Comm_size(MPI_COMM_WORLD, &procNum);
   MPI_Comm_rank(MPI_COMM_WORLD, &procRank);
   MPI_Get_processor_name(procName, &nameLen);
   MPI_Status Status;

   fprintf(stderr, "Process %d on %s\n", procRank, procName);

   if(procRank == 0){

      arrayToSort = (int*)malloc(size * sizeof(int));

      for(i = 0; i < size; ++i){
         arrayToSort[i] = rand();
      }

      /*for(i = 0; i < size; ++i){
         printf("%d ", arrayToSort[i]);
      }
      printf("\n\n");*/
   }

   startTime = MPI_Wtime();

   if((size < procNum) && (procRank == 0)){

      QuickSort(arrayToSort, 0, size - 1);

   } else if (size >= procNum) {
        int* sendCount = (int*)malloc(procNum * sizeof(int));
        int* sendPoint = (int*)malloc(procNum * sizeof(int));

        for(i = 0; i < procNum; ++i){
           sendCount[i] = size / procNum;
           sendPoint[i] = i * sendCount[i];
        }
        sendCount[procNum - 1] += size % procNum;

        int* subArray = (int*)malloc(sendCount[procRank]* sizeof(int));

        MPI_Scatterv(arrayToSort, sendCount, sendPoint, MPI_INT, subArray, sendCount[procRank], MPI_INT, 0, MPI_COMM_WORLD);

        if(procRank == 0){
           free(arrayToSort);
        }

        QuickSort(subArray, 0, sendCount[procRank] - 1);

        int* downArray;
        int* upArray;
        int* recvArray;
        int pivot = 0, sendLength = sendCount[procRank], downArrayLength = 0, upArrayLength = 0, recvArrayLength = 0;
        int procInd = procNum;

        free(sendCount);
        free(sendPoint);

        while(procInd > 1){
           for(i = 0; i < procNum; i += procInd){
              if(procRank == i){
                 pivot = subArray[sendLength/2];
                 for(j = i + 1; j < i + procInd; ++j){
                    MPI_Send(&pivot, 1, MPI_INT, j, 0, MPI_COMM_WORLD);
                 }
              } else if((procRank > i) && (procRank < i + procInd)){
                   MPI_Recv(&pivot, 1, MPI_INT, i, 0, MPI_COMM_WORLD, &Status);
              }

              for(j = i; j < i + procInd; ++j){
                 if(procRank == j){
                    Divide(subArray, &downArray, &upArray, sendLength, &downArrayLength, &upArrayLength, pivot);
                    free(subArray);
                 }
              }

              for(j = i; j < i + procInd / 2; ++j){
                 if(procRank == j){
                    MPI_Send(&upArrayLength, 1, MPI_INT, i + procInd - 1 - j % procInd, 0, MPI_COMM_WORLD);
                    MPI_Send(upArray, upArrayLength, MPI_INT, i + procInd - 1 - j % procInd, 0, MPI_COMM_WORLD);
                    MPI_Recv(&recvArrayLength, 1, MPI_INT, i + procInd - 1 - j % procInd, 0, MPI_COMM_WORLD, &Status);
                    recvArray = (int*)malloc(recvArrayLength * sizeof(int));
                    MPI_Recv(recvArray, recvArrayLength, MPI_INT, i + procInd - 1 - j % procInd, 0, MPI_COMM_WORLD, &Status);

                    free(upArray);

                    subArray = Merge(downArray, recvArray, downArrayLength, recvArrayLength);

                    free(downArray);
                    free(recvArray);

                    sendLength = downArrayLength + recvArrayLength;

                 } else if(procRank == i + procInd - 1 - j % procInd){
                      MPI_Recv(&recvArrayLength, 1, MPI_INT, j, 0, MPI_COMM_WORLD, &Status);
                      recvArray = (int*)malloc(recvArrayLength * sizeof(int));
                      MPI_Recv(recvArray, recvArrayLength, MPI_INT, j, 0, MPI_COMM_WORLD, &Status);
                      MPI_Send(&downArrayLength, 1, MPI_INT, j, 0, MPI_COMM_WORLD);
                      MPI_Send(downArray, downArrayLength, MPI_INT, j, 0, MPI_COMM_WORLD);

                      free(downArray);

                      subArray = Merge(upArray, recvArray, upArrayLength, recvArrayLength);

                      free(upArray);
                      free(recvArray);

                      sendLength = upArrayLength + recvArrayLength;
                   }
              }
           }
           procInd /= 2;
           MPI_Barrier(MPI_COMM_WORLD);
        }

        if(procRank == 0){
           sendCount = (int*)malloc(procNum * sizeof(int));
        }

        MPI_Gather(&sendLength, 1, MPI_INT, sendCount, 1, MPI_INT, 0, MPI_COMM_WORLD);

        if(procRank == 0){
           sendPoint = (int*)malloc(procNum * sizeof(int));

           sendPoint[0] = 0;
           for(i = 1; i < procNum; ++i){
              sendPoint[i] = sendPoint[i - 1] + sendCount[i - 1];
           }

           arrayToSort = (int*)malloc(size * sizeof(int));
        }

        MPI_Gatherv(subArray, sendLength, MPI_INT, arrayToSort, sendCount, sendPoint, MPI_INT, 0, MPI_COMM_WORLD);

        if(procRank == 0){
           free(sendPoint);
           free(sendCount);
        }

        free(subArray);
   }

   endTime = MPI_Wtime();

   if(procRank == 0){
      /*for(i = 0; i < size; ++i){
         printf("%d ", arrayToSort[i]);
      }
      printf("\n");*/

      printf("\nTime = %f\n", (endTime - startTime));
      free(arrayToSort);
   }

   MPI_Finalize();
   return 0;
}
