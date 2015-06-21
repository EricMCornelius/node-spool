var gulp = require('gulp');
var istanbul = require('gulp-istanbul');
var mocha = require('gulp-mocha');
var jshint = require('gulp-jshint');
var stylish = require('jshint-stylish');
var exit = require('gulp-exit');

function instrument() {
  return gulp.src(['lib/**/*.js'])
    .pipe(jshint())
    .pipe(jshint.reporter(stylish))
    .pipe(istanbul())
    .pipe(istanbul.hookRequire());
}

function run_tests() {
  process.env.NODE_ENV = 'test';
  return gulp.src(['test/*.js'])
    .pipe(mocha({reporter: 'spec'}))
    .pipe(istanbul.writeReports())
    .pipe(exit());
}

gulp.task('instrument', instrument);
gulp.task('run_tests', ['instrument'], run_tests);

gulp.task('test', ['run_tests']);
gulp.task('default', ['test']);
