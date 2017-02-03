set more off
clear all

global home /data/qcew/
global rawdata ${home}raw/
global cleandata ${home}clean/


* DOWNLOAD
* NAICS-based
forvalues year = 1990/2016 {
	di "Downloading NAICS - `year'"
	copy http://data.bls.gov/cew/data/files/`year'/csv/`year'_qtrly_singlefile.zip ${rawdata}, replace
}

* SIC-based
forvalues year = 1975/2000 {
	copy http://data.bls.gov/cew/data/files/`year'/sic/csv/sic_`year'_qtrly_singlefile.zip ${rawdata}, replace
	https://www.bls.gov/cew/data/files/1975/sic/csv/sic_1975_qtrly_singlefile.zip
}



* Process NAICS-based
local begyear 2014
local endyear 2016
local endyear_endqtr 2

local nationallevels agglvl_code >= 10 & agglvl_code <= 28
local csalevels agglvl_code == 30
local msalevels agglvl_code >= 40 & agglvl_code <= 48
local statelevels agglvl_code >= 50 & agglvl_code <= 64
local countylevels agglvl_code >= 70 & agglvl_code <= 78
local microsalevels agglvl_code == 80

forvalues year = `begyear'/`endyear' {
	di "Working on `year'"
	unzipfile ${rawdata}`year'_qtrly_singlefile.zip, replace
	if `year' == `endyear' local endqtr `endyear_endqtr'
	else local endqtr 4
	insheet using `year'.q1-q`endqtr'.singlefile.csv, clear

	tempfile qcewdata
	save `qcewdata'

	foreach geocat in national state county {
		use if ``geocat'levels' using `qcewdata', clear
		compress
		save qcew_`geocat'_`year'.dta, replace
		zipfile qcew_`geocat'_`year'.dta, saving(${cleandata}qcew_`geocat'_`year'.dta.zip, replace)
		erase qcew_`geocat'_`year'.dta
	}
	erase `year'.q1-q`endqtr'.singlefile.csv
}



* Process SIC-based
local begyear 1975
local endyear 2000

local nationallevels agglvl_code >= 1 & agglvl_code <= 11
local msalevels agglvl_code >= 12 & agglvl_code <= 17
local statelevels agglvl_code >= 18 & agglvl_code <= 25
local countylevels agglvl_code >= 26 & agglvl_code <= 31

forvalues year = `begyear'/`endyear' {
	di "Working on `year'"
	unzipfile ${rawdata}sic_`year'_qtrly_singlefile.zip, replace
	insheet using sic.`year'.q1-q4.singlefile.csv, clear
	tempfile qcewdata
	save `qcewdata'

	foreach geocat in national state county {
		use if ``geocat'levels' using `qcewdata', clear
		compress
		save qcew_sic_`geocat'_`year'.dta, replace
		zipfile qcew_sic_`geocat'_`year'.dta, saving(${cleandata}qcew_sic_`geocat'_`year'.dta.zip, replace)
		erase qcew_sic_`geocat'_`year'.dta
	}
	erase sic.`year'.q1-q4.singlefile.csv
}
