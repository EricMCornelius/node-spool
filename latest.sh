#!/bin/bash

find spool -type f -exec basename {} \; | sort
