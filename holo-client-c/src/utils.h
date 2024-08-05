#ifndef _UTILS_H_
#define _UTILS_H_

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <stdint.h>

#define MALLOC(n, type) \
    ((type *)malloc((n) * sizeof(type)))

#define FREE(ptr)           \
    do {                    \
        if (ptr != NULL) {  \
            free(ptr);      \
        }                   \
    } while (0)

#define SQL_STR_DOUBLE(ch, escape_backslash)	\
	((ch) == '\'' || ((ch) == '\\' && (escape_backslash)))
#define ESCAPE_STRING_SYNTAX	'E'

char* deep_copy_string(const char*);
void deep_copy_string_to(const char*, char*, int);
long long get_time_usec();
struct timespec get_out_time(long long);
struct timespec get_time_spec_from_ms(long long);
char* itoa(int);
int len_of_int(int);
char* quote_table_name(const char*, const char*);
char* quote_identifier(const char*);
size_t quote_literal_internal(char *dst, const char *src, size_t len);
char* quote_literal_cstr(const char*);
int get_max_pow(int);

long current_time_ms();
void endian_swap(void*, int);
void to_lower_case(char*, int len);
bool compare_strings(const char *str1, const char *str2);

char* int16toa(int16_t);
char* int32toa(int32_t);
char* int64toa(int64_t);
char* btoa(bool);
char* ftoa(float);
char* dtoa(double);

char* int32_array_toa(int32_t*, int);
char* int64_array_toa(int64_t*, int);
char* bool_array_toa(bool*, int);
char* float_array_toa(float*, int);
char* double_array_toa(double*, int);
char* text_array_toa(char**, int);
#endif