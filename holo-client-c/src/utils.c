#include "utils.h"
#include "sys/time.h"
#include "logger.h"
#include "time.h"

char* deep_copy_string(const char* s) {
    int length;
    if (s == NULL) return NULL;
    length = strlen(s) + 1;
    char* copied = (char*) malloc (length);
    strncpy(copied, s, length);
    return copied;
}

void deep_copy_string_to(const char* s, char* dst){
    if (dst == NULL) return;
    if (s == NULL) {
        dst[0] = '\0';
        return;
    }
    strncpy(dst, s, strlen(s) + 1);
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
    struct timespec now;
    timespec_get(&now, TIME_UTC);
    return ((long) now.tv_sec) * 1000 + ((long) now.tv_nsec) / 1000000;
}

void endian_swap(void* src, int length){
    for (int i = 0;i < length/2; i++){
        u_int8_t t = *((u_int8_t*)(src + i));
        *((u_int8_t*)(src + i)) = *((u_int8_t*)(src + length - 1 - i));
        *((u_int8_t*)(src + length - 1 - i)) = t;
    }
}

void to_lower_case(char* str){
    while (*str){
        if (*str >= 'A' && *str <= 'Z') *str -= 'A' - 'a';
        ++str;
    }
}

char* quote_identifier(char* name) {
    char* ret;
    int length = strlen(name), pos = 0, i;
    bool safe = true;
    int nquotes = 0;
    char c;
    for (i = 0; i < length; i++) {
        c = name[i];
        if ((c >= 'a' && c <= 'z') || c == '_') {
            continue;
        } else if (c >= 'A' && c <= 'Z') {
            safe = false;
        } else if (c >= '0' && c <= '9') {
            if (i == 0) {
                safe = false;
            }
        } else if (c == '"') {
            nquotes++;
            safe = false;
        } else {
            safe = false;
        }
    }
    if(safe) {
        return deep_copy_string(name);
    }
    ret = MALLOC(length + nquotes + 3, char);
    ret[pos] = '"'; pos++;
    for (i = 0; i < length; i++)  {
        ret[pos] = name[i];
        pos++;
        if (name[i] == '"') {
            ret[pos] = '"';
            pos++;
        }
    }
    ret[pos] = '"';
    pos++;
    ret[pos] = '\0';
    return ret;
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
    int len = snprintf(NULL, 0, "%lld", i);
    char *result = MALLOC(len + 1, char);
    snprintf(result, len + 1, "%lld", i);
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
        len += snprintf(NULL, 0, "%lld", array[i]);
    }
    len = len + 2 + n;
    result = MALLOC(len, char);
    for (int i = 0; i < n; i++) {
        if (i == 0) cur += snprintf(result, len, "{%lld", array[i]);
        else cur += snprintf(result+cur, len-cur, ",%lld", array[i]);
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