#!/bin/sh

source_path_to_replace=$1
new_source_path_value=$2
package_name=$3
coverage_report_path=$4

sed -i"" -e "s|$source_path_to_replace|$new_source_path_value|" $coverage_report_path
sed -i"" -e "s| name=\"virtualenv.*\"| name=\"$package_name\"|" $coverage_report_path
sed -i"" -e "s| filename=\"virtualenv.*/$package_name/| filename=\"|" $coverage_report_path
