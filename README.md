# HW4
This repository is for Distributed System Homework 4

How to:
# Terminal 1
go run . --ID=1 --Port=:5050 --OtherPorts=:5051,:5052
# Terminal 2
go run . --ID=2 --Port=:5051 --OtherPorts=:5050,:5052
# Terminal 3
go run . --ID=3 --Port=:5052 --OtherPorts=:5050,:5051

Please note that after doing this, you will need to wait for >2 minutes for the servers to start.