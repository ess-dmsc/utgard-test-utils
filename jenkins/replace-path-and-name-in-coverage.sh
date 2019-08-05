#!/bin/sh

source_path_to_replace=$1
package_name=$2
coverage_report_path=$3

sed -i"" -e "s|$source_path_to_replace|.|" $coverage_report_path
sed -i"" -e "s| name=\"virtualenv.*\"| name=\"$package_name\"|" $coverage_report_path
sed -i"" -e "s| filename=\"virtualenv.*/| filename=\"|" $coverage_report_path
