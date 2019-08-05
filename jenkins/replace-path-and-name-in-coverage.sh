#!/bin/sh

path_to_replace=$1
package_name=$2
coverage_report_path=$3

sed -i"" -e "s|$path_to_replace|.|" $coverage_report_path
sed -i"" -e "s| name=\"\.\"| name=\"$package_name\"|" $coverage_report_path
