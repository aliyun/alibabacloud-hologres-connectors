#ifndef _ACTION_H_
#define _ACTION_H_

#include "request_private.h"
#include "batch.h"
#include "../include/request.h"

typedef struct _Action{
    int type; 
} Action;

typedef struct _ActionItem {
    dlist_node list_node;
    Action* action;
} ActionItem;

ActionItem* create_action_item(Action*);

typedef enum _ActionStatus {
    SUCCESS,
	FAILURE_NEED_RETRY,
	FAILURE_NOT_NEED_RETRY,
    FAILURE_TO_BE_DETERMINED
} ActionStatus;

typedef struct _MetaAction {
    int type; //0
    Meta meta;
} MetaAction;

MetaAction* holo_client_new_meta_action(Meta);
void holo_client_destroy_meta_action(MetaAction*);

typedef struct _MutationAction {
    int type; //1
    dlist_head requests; //list of mutation requests
    int numRequests;
    Future* future;
} MutationAction;

MutationAction* holo_client_new_mutation_action();
void mutation_action_add_mutation(MutationAction*, Mutation);
void holo_client_destroy_mutation_action(MutationAction*);

typedef struct _SqlAction {
    int type; //2
    Sql sql;
} SqlAction;

SqlAction* holo_client_new_sql_action(Sql);
void holo_client_destroy_sql_action(SqlAction*);

typedef struct _GetAction {
    int type; //3
    dlist_head requests; //list of get requests
    int numRequests;
    TableSchema* schema;
} GetAction;
GetAction* holo_client_new_get_action();
void holo_client_destroy_get_action(GetAction*);
void get_action_add_request(GetAction*, Get);
void abort_get_action(GetAction*);

#endif