# twc_verification
**Verification of The Weather Company forecasts**

This repository stores the files used in performing a simple verification of the twc forecasts that are stored in the kepler51 aws S3 bucket with the corresponding observations.

The verify_twc_config.yml file has the necessary configuration settings.

- Verify_twc_lib.py contains the main extraction functions using Amazon S3 Select - as described here https://aws.amazon.com/blogs/storage/querying-data-without-servers-or-databases-using-amazon-s3-select/

- Verify_twc.py parses the list of given id codes and calls the extraction and plotting functions from the library file for each location

- The forecasts file contains 10 days of forecasts for each location. The initialisation time is given in the yml file. This date is then used to extract the correct forecast and for any number of specified days and hours. The default is 24 hours and 10 days. It should be noted that the init time is 1 hour after the time given in the filename. These files are large ~ 56MB in compressed format.

- The observations are stored in a similarly named file with parameter names that are exactly the same. Each file has one observation for each location for one hour and so are much smaller ~ 375kb.

- Both the above mentioned functions extract information and store it in a dataframe that is then plotted. The plots are saved as a pdf file. The root mean square error is also calculated and it written into the title of the plot.

- It is expected the user will have their own aws access credentials for the Kepler51 buckets
