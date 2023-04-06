#include "utils.h"
#include "sys/time.h"
#include "logger.h"
#include "time.h"
#include "inttypes.h"

char* deep_copy_string(const char* s) {
    int length;
    if (s == NULL) return NULL;
    length = strlen(s) + 1;
    char* copied = (char*)malloc(length);
    strncpy(copied, s, length);
    return copied;
}

void deep_copy_string_to(const char* s, char* dst, int len){
    if (dst == NULL) return;
    if (s == NULL) {
        return;
    }
    strncpy(dst, s, len);
}

long long get_time_usec(){
    long long ret;
    struct timeval tv;
    gettimeofday(&tv, NULL);
    ret = tv.tv_sec * 1000000 + tv.tv_usec;
    return ret;
}

struct timespec get_out_time(long long time_interval_ms){
    struct timeval now;
    struct timespec outtime;
 
    gettimeofday(&now, NULL);
    outtime.tv_sec = now.tv_sec + time_interval_ms / 1000;
    outtime.tv_nsec = now.tv_usec * 1000 + (time_interval_ms % 1000) * 1000;
    if (outtime.tv_nsec > 1000000000){
        outtime.tv_sec++;
        outtime.tv_nsec -= 1000000000;
    }

    return outtime;
}

char* itoa(int x){
    int length = len_of_int(x);
    char* str = MALLOC(length + 1, char);
    snprintf(str, length + 1, "%d", x);
    return str;
}

int len_of_int(int x){
    if (x < 0){
        LOG_ERROR("Int here should not be negative.");
        return 0;
    }
    if (x < 10) return 1;
    if (x < 100) return 2;
    if (x < 1000) return 3;
    if (x < 10000) return 4;
    if (x < 100000) return 5;
    if (x < 1000000) return 6;
    if (x < 10000000) return 7;
    LOG_ERROR("Int here should not be so large.");
    return 0;
}

long current_time_ms() {
    struct timeval now;
    gettimeofday(&now, NULL);
    return ((long) now.tv_sec) * 1000 + ((long) now.tv_usec) / 1000;
}

void endian_swap(void* src, int length){
    for (int i = 0;i < length/2; i++){
        uint8_t t = *((uint8_t*)(src + i));
        *((uint8_t*)(src + i)) = *((uint8_t*)(src + length - 1 - i));
        *((uint8_t*)(src + length - 1 - i)) = t;
    }
}

void to_lower_case(char* str, int len){
    while (len--){
        if (*str >= 'A' && *str <= 'Z') *str -= 'A' - 'a';
        ++str;
    }
}

int get_max_pow(int num) {
    int flag = num & (num - 1);
    if (flag == 0)
        flag = num;
    int res = 1;
    while (flag >>= 1) {
        res <<= 1;
    }
    return res;
}

char* quote_table_name(const char* schema_name, const char* table_name) {
    const char* s1 = quote_identifier(schema_name);
    const char* s2 = quote_identifier(table_name);
    size_t l1 = strlen(s1);
    size_t l2 = strlen(s2);
    size_t len = l1 + l2 + 1 + 1;
    char* s = MALLOC(len, char);
    char* p = s;
    memcpy(p, s1, l1);
    p += l1;
    *p++ = '.';
    memcpy(p, s2, l2);
    p += l2;
    *p = '\0';
    if (s1 != schema_name) {
        FREE((void*)s1);
    }
    if (s2 != table_name) {
        FREE((void*)s2);
    }
    return s;
}

/*
 * Copy from ruleutils.c, remove ScanKeyWords
 */
char* quote_identifier(const char *ident)
{
	/*
	 * Can avoid quoting if ident starts with a lowercase letter or underscore
	 * and contains only lowercase letters, digits, and underscores, *and* is
	 * not any SQL keyword.  Otherwise, supply quotes.
	 */
	int			nquotes = 0;
	bool		safe;
	const char *ptr;
	char	   *result;
	char	   *optr;
	safe = ((ident[0] >= 'a' && ident[0] <= 'z') || ident[0] == '_');
	for (ptr = ident; *ptr; ptr++)
	{
		char		ch = *ptr;
		if ((ch >= 'a' && ch <= 'z') ||
			(ch >= '0' && ch <= '9') ||
			(ch == '_'))
		{
			/* okay */
		}
		else
		{
			safe = false;
			if (ch == '"')
				nquotes++;
		}
	}
	if (safe)
		return deep_copy_string(ident);			/* no change needed */
	result = MALLOC(strlen(ident) + nquotes + 2 + 1, char);
	optr = result;
	*optr++ = '"';
	for (ptr = ident; *ptr; ptr++)
	{
		char		ch = *ptr;

		if (ch == '"')
			*optr++ = '"';
		*optr++ = ch;
	}
	*optr++ = '"';
	*optr = '\0';
	return result;
}

/*
 * Copy from quote.c
 */
size_t quote_literal_internal(char *dst, const char *src, size_t len)
{
	const char *s;
	char	   *savedst = dst;
	for (s = src; s < src + len; s++)
	{
		if (*s == '\\')
		{
			*dst++ = ESCAPE_STRING_SYNTAX;
			break;
		}
	}
	*dst++ = '\'';
	while (len-- > 0)
	{
		if (SQL_STR_DOUBLE(*src, true))
			*dst++ = *src;
		*dst++ = *src++;
	}
	*dst++ = '\'';
	return dst - savedst;
}

/*
 * Copy from quote.c
 */
char* quote_literal_cstr(const char *rawstr)
{
	char	   *result;
	int			len;
	int			newlen;

	len = strlen(rawstr);
	/* We make a worst-case result area; wasting a little space is OK */
	result = MALLOC(len * 2 + 3 + 1, char);
	newlen = quote_literal_internal(result, rawstr, len);
	result[newlen] = '\0';
	return result;
}

char* int16toa(int16_t i) {
    int len = snprintf(NULL, 0, "%d", i);
    char *result = MALLOC(len + 1, char);
    snprintf(result, len + 1, "%d", i);
    return result;
}
char* int32toa(int32_t i) {
    int len = snprintf(NULL, 0, "%d", i);
    char *result = MALLOC(len + 1, char);
    snprintf(result, len + 1, "%d", i);
    return result;
}
char* int64toa(int64_t i) {
    int len = snprintf(NULL, 0, "%" PRId64"", i);
    char *result = MALLOC(len + 1, char);
    snprintf(result, len + 1, "%" PRId64"", i);
    return result;
}
char* btoa(bool b) {
    int len = snprintf(NULL, 0, "%d", b);
    char *result = MALLOC(len + 1, char);
    snprintf(result, len + 1, "%d", b);
    return result;
}
char* ftoa(float f) {
    int len = snprintf(NULL, 0, "%f", f);
    char *result = MALLOC(len + 1, char);
    snprintf(result, len + 1, "%f", f);
    return result;
}
char* dtoa(double d) {
    int len = snprintf(NULL, 0, "%f", d);
    char *result = MALLOC(len + 1, char);
    snprintf(result, len + 1, "%f", d);
    return result;
}

char* int32_array_toa(int32_t* array, int n) {
    int len = 0, cur = 0;
    char* result;
    for (int i = 0; i < n; i++) {
        len += snprintf(NULL, 0, "%d", array[i]);
    }
    len = len + 2 + n;
    result = MALLOC(len, char);
    for (int i = 0; i < n; i++) {
        if (i == 0) cur += snprintf(result, len, "{%d", array[i]);
        else cur += snprintf(result+cur, len-cur, ",%d", array[i]);
    }
    cur += snprintf(result+cur, len-cur, "%s", "}");
    return result;
}
char* int64_array_toa(int64_t* array, int n) {
    int len = 0, cur = 0;
    char* result;
    for (int i = 0; i < n; i++) {
        len += snprintf(NULL, 0, "%" PRId64"", array[i]);
    }
    len = len + 2 + n;
    result = MALLOC(len, char);
    for (int i = 0; i < n; i++) {
        if (i == 0) cur += snprintf(result, len, "{%" PRId64"", array[i]);
        else cur += snprintf(result+cur, len-cur, ",%" PRId64"", array[i]);
    }
    cur += snprintf(result+cur, len-cur, "%s", "}");
    return result;
}
char* bool_array_toa(bool* array, int n) {
    int len = 0, cur = 0;
    char* result;
    for (int i = 0; i < n; i++) {
        len += snprintf(NULL, 0, "%d", array[i]);
    }
    len = len + 2 + n;
    result = MALLOC(len, char);
    for (int i = 0; i < n; i++) {
        if (i == 0) cur += snprintf(result, len, "{%d", array[i]);
        else cur += snprintf(result+cur, len-cur, ",%d", array[i]);
    }
    cur += snprintf(result+cur, len-cur, "%s", "}");
    return result;
}
char* float_array_toa(float* array, int n) {
    int len = 0, cur = 0;
    char* result;
    for (int i = 0; i < n; i++) {
        len += snprintf(NULL, 0, "%f", array[i]);
    }
    len = len + 2 + n;
    result = MALLOC(len, char);
    for (int i = 0; i < n; i++) {
        if (i == 0) cur += snprintf(result, len, "{%f", array[i]);
        else cur += snprintf(result+cur, len-cur, ",%f", array[i]);
    }
    cur += snprintf(result+cur, len-cur, "%s", "}");
    return result;
}
char* double_array_toa(double* array, int n) {
    int len = 0, cur = 0;
    char* result;
    for (int i = 0; i < n; i++) {
        len += snprintf(NULL, 0, "%f", array[i]);
    }
    len = len + 2 + n;
    result = MALLOC(len, char);
    for (int i = 0; i < n; i++) {
        if (i == 0) cur += snprintf(result, len, "{%f", array[i]);
        else cur += snprintf(result+cur, len-cur, ",%f", array[i]);
    }
    cur += snprintf(result+cur, len-cur, "%s", "}");
    return result;
}
char* text_array_toa(char** array, int n) {
    int len = 0, cur = 0;
    char* result;
    for (int i = 0; i < n; i++) {
        len += strlen(array[i]);
    }
    len = len + 2 + n;
    result = MALLOC(len, char);
    for (int i = 0; i < n; i++) {
        if (i == 0) cur += snprintf(result, len, "{%s", array[i]);
        else cur += snprintf(result+cur, len-cur, ",%s", array[i]);
    }
    cur += snprintf(result+cur, len-cur, "%s", "}");
    return result;
}