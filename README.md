# Euclid Dataflow Simulator [![Build Status](https://travis-ci.org/cansik/eucliddatasimulator.svg)](https://travis-ci.org/cansik/eucliddatasimulator) [![Code Climate](https://codeclimate.com/github/cansik/eucliddatasimulator/badges/gpa.svg)](https://codeclimate.com/github/cansik/eucliddatasimulator)

![Euclid Dataflow Simulator](images/small.png)

### Project
The Euclid project was launched by the University of Applied Sciences Northwestern Switzerland in order to support a research project of the same name. The research project is a project that was launched by ESA and aims to send a satellite into space to create a map of dark materia. Our focus is mainly on the processing of the returned data from the satellite. Since these data are analyzed by means of complex algorithms in larger data centers, the ground infrastructure should have been tested enough to deal with these dimensions of data.

Using the IP-5 researchers can easily and quickly generate test data and test the infrastructure. This test data must be modeled as closely as possible to the real conditions, but must bring no added value.

At the end of the project a software is to be built, with which you can parameterize input data and incurring test data.

### Euclid Stubs Generator
1. Prepare destination directory (copy generators and euclidwf files)
2. Load and parse executables
3. Create a folder for every executable
4. Create a textexec for every executable

### Euclid Performance
Uses computer resources as defined:

- CPU / Cores
- IO Operations
- RAM Usage

```python
from RessourceUser import RessourceUser

user = RessourceUser()

user.use_cpu(0)
user.use_memory(12000)
user.use_io(0, 0)

user.start(120)
print("finished!")
```

*IP5 Project of Andreas Lüscher and Florian Bruggisser*
