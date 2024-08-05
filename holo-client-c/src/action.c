#include "action.h"
#include "utils.h"
#include "logger_private.h"

ActionItem* create_action_item(Action* action) {
    ActionItem* item = MALLOC(1, ActionItem);
    item->action = action;
    return item;
}

MetaAction* holo_client_new_meta_action(Meta meta) {
    MetaAction* action = MALLOC(1, MetaAction);
    action->type = 0;
    action->meta = meta;
    return action;
}

void holo_client_destroy_meta_action(MetaAction* action) {
    //holo_client_destroy_meta_request(action->meta);
    FREE(action);
    action = NULL;
}

MutationAction* holo_client_new_mutation_action(){
    MutationAction* action = MALLOC(1, MutationAction);
    action->type = 1;
    dlist_init(&(action->requests));
    action->numRequests = 0;
    action->future = create_future();
    return action;
}

void mutation_action_add_mutation(MutationAction* action, HoloMutation mutation){
    dlist_push_tail(&(action->requests),&(create_mutation_item(mutation)->list_node));
    action->numRequests += 1;
}

void holo_client_destroy_mutation_action(MutationAction* action){
    dlist_mutable_iter miter;
    MutationItem* item;
    dlist_foreach_modify(miter, &(action->requests)) {
        item = dlist_container(MutationItem, list_node, miter.cur);
        holo_client_destroy_mutation_request(item->mutation);
        dlist_delete(miter.cur);
        FREE(item); 
    }
    destroy_future(action->future);
    FREE(action);
    action = NULL;
}

GetAction* holo_client_new_get_action() {
    GetAction* action = MALLOC(1, GetAction);
    action->type = 3;
    dlist_init(&(action->requests));
    action->numRequests = 0;
    return action;
}

void holo_client_destroy_get_action(GetAction* action) {
    dlist_mutable_iter miter;
    GetItem* item;
    dlist_foreach_modify(miter, &(action->requests)) {
        item = dlist_container(GetItem, list_node, miter.cur);
        // holo_client_destroy_get_request(item->get);
        dlist_delete(miter.cur);
        FREE(item); 
    }
    FREE(action);
    action = NULL;
}

void abort_get_action(GetAction* action) {
    dlist_iter iter;
    GetItem* item;
    dlist_foreach(iter, &(action->requests)) {
        item = dlist_container(GetItem, list_node, iter.cur);
        if (!item->get->future->completed) complete_future(item->get->future, NULL);
    }
}

void get_action_add_request(GetAction* action, HoloGet get) {
    dlist_push_tail(&(action->requests),&(create_get_item(get)->list_node));
    action->numRequests++;
}

SqlAction* holo_client_new_sql_action(Sql sql) {
    SqlAction* action = MALLOC(1, SqlAction);
    action->type = 2;
    action->sql = sql;
    return action;
}

void holo_client_destroy_sql_action(SqlAction* action) {
    //holo_client_destroy_sql_request(action->sql);
    FREE(action);
    action = NULL;
}