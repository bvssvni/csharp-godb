csharp-godb
===========

A self-maintained data base format.  
BSD license.  
For version log, view the individual files.  

#What is GODB?

GODB = Great Object Data Base

It stores huge binary objects as lists of blocks.  
Each object can has a unique identifier of type int64.  

The lists of blocks are stored in the block with oid 0.  
This is the first block in the file.  

For most applications, you wanna use 'OdbLib.BinaryDatabase'.  
This is a easy-to-use wrapper that reads and stores blobs identified by strings.  

If you worry about data corruption under power failure,  
or need to do huge data processing that can be rolled back,  
use 'OdbLib.GTransaction'.  

If you need streaming to a binary object, use 'OdbLib.GOdbStream'.  

