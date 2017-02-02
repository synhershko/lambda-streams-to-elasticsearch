#!/bin/bash

version=`cat package.json | grep version | cut -d: -f2 | sed -e "s/\"//g" | sed -e "s/ //g" | sed -e "s/\,//g"`

functionName=LambdaStreamToES
filename=$functionName-$version.zip
region=us-east-1

npm install

rm $filename 2>&1 >> /dev/null

zip -x \*node_modules/protobufjs/tests/\* -r $filename index.js router.js transformer.js constants.js lambda.json package.json node_modules/ README.md LICENSE && mv -f $filename dist/$filename

if [ ! -z "$1" ]; then
    aws s3 cp dist/$filename $1/$filename
fi
