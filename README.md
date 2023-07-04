<div align="center">
  <h3 align="center">FPL and Understat Data Processor</h3>

  <p align="center">
    A data processing and pipeline project written in Scala and Spark, which takes data created by the fpl-data-extractor 
    and understat-data-extractor projects, transforms it by joining it together and creating some calculated columns and 
    writes it to csv files.
  </p>
</div>

<!-- ABOUT THE PROJECT -->
## About The Project

The intention of this project is to create data that will be used downstream in a machine learning project.
</br>
</br>
Fpl data is provided by the [fpl-data-extractor](https://github.com/TheAlchemyIndex/fpl-data-extractor) project, and 
Understat data is provided by the [understat-data-extractor](https://github.com/TheAlchemyIndex/understat-data-extractor) 
project.
</br>

## Getting Started

### Prerequisites

* Scala - This project has been built and tested using Scala 2.12.17 and Java 1.8
* Spark - This project has been built and tested using Spark 3.4.0

<!-- USAGE EXAMPLES -->
## Usage

After ensuring that Scala and Spark are installed, clone this repo and run the main method in Main.scala.
</br>
</br>
The config parameters for this project can be accessed in src/main/resources/fpl_understat_processor.conf. The default 
fileName variables are set to the default files found in the data folder. If new files are created using the fpl-data-extractor
or understat-data-extractor projects, load these files into the data folder and update the fileName variables in the config file
to match the new data files.
</br>
</br>
Hadoop may also be required, if so, instructions to install Hadoop can be found through various online sources. Windows 
users may have issues with Hadoop installation, in particular with a winutils file, binaries for your relevant Hadoop
version can be found here - (https://github.com/steveloughran/winutils).

<!-- LICENSE -->
## License

Distributed under the MIT License. See `LICENSE` for more information.

<!-- CONTACT -->
## Contact

TheAlchemyIndex - [LinkedIn](https://www.linkedin.com/in/vaughana)

<!-- ACKNOWLEDGMENTS -->
## Acknowledgments

* [Best-README-Template - othneildrew](https://github.com/othneildrew/Best-README-Template)