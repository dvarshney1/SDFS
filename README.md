# Simple Distributed Filesystem using GOSSIP membership protocol

We have implemented a Simple distributed file system in this project (SDFS). We are using GOSSIP as our membership protocol. We used Python3 for this assignment.

## Usage

```bash
./do_everything.sh
```
The above command will start a python process that will accept operations in the following format:
- put [localfilename] [sdfsfilename]
- get [sdfsfilename] [localfilename]
- delete [sdfsfilename]
- ls [sdfsfilename]
- store

Each local process on starting will create a directory called `sdfs` which will store all distributed files that are stored in the current server.

## Testing

To create some randomly generated test files, you can run the following script.
```bash
./create_test_files.sh
```
This will create files of different sizes (1KB, 10KB etc.) in the `test_files` directory

### Note
All our constants are defined in file consts.py and can be used to tweak the system based on constraints