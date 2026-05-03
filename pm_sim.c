#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <stdbool.h>
#define MAX_PROCESSES 64
typedef enum {
    STATE_RUNNING,
    STATE_BLOCKED,
    STATE_ZOMBIE,
    STATE_TERMINATED
} ProcessState;
typedef struct {
    int pid;
    int ppid;
    ProcessState state;
    int exit_status;
} PCB;
PCB process_table[MAX_PROCESSES];
int next_pid = 2;
pthread_mutex_t pt_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t wait_cv = PTHREAD_COND_INITIALIZER;
pthread_cond_t monitor_cv = PTHREAD_COND_INITIALIZER;
bool monitor_update_flag = false;
char monitor_message[256] = "Initial Process Table";
FILE *snapshot_file;
const char* state_to_string(ProcessState state) {
    switch(state) {
        case STATE_RUNNING: return "RUNNING";
        case STATE_BLOCKED: return "BLOCKED";
        case STATE_ZOMBIE: return "ZOMBIE";
        case STATE_TERMINATED: return "TERMINATED";
        default: return "UNKNOWN";
    }
}
void trigger_monitor(const char* message) {
    strncpy(monitor_message, message, sizeof(monitor_message) - 1);
    monitor_update_flag = true;
    pthread_cond_signal(&monitor_cv);
}
void pm_ps() {
    fprintf(snapshot_file, "%s\n", monitor_message);
    fprintf(snapshot_file, "PID\t\tPPID\t\tSTATE\t\tEXIT_STATUS\n");
    fprintf(snapshot_file, "----------------------------------------------\n");
    for (int i = 0; i < MAX_PROCESSES; i++) {
        if (process_table[i].pid != 0 && process_table[i].state != STATE_TERMINATED) {
            fprintf(snapshot_file, "%d\t\t%d\t\t%s\t\t", 
                    process_table[i].pid, process_table[i].ppid, state_to_string(process_table[i].state));
            if (process_table[i].state == STATE_ZOMBIE) {
                fprintf(snapshot_file, "%d\n", process_table[i].exit_status);
            } else {
                fprintf(snapshot_file, "-\n");
            }
        }
    }
    fprintf(snapshot_file, "\n");
    fflush(snapshot_file);
}
void pm_fork(int parent_pid, int thread_id) {
    pthread_mutex_lock(&pt_mutex);
    int slot = -1;
    for (int i = 0; i < MAX_PROCESSES; i++) {
        if (process_table[i].pid == 0 || process_table[i].state == STATE_TERMINATED) {
            slot = i;
            break;
        }
    }
    if (slot != -1) {
        process_table[slot].pid = next_pid++;
        process_table[slot].ppid = parent_pid;
        process_table[slot].state = STATE_RUNNING;
        process_table[slot].exit_status = 0;
        char msg[256];
        snprintf(msg, sizeof(msg), "Thread %d calls pm_fork %d", thread_id, parent_pid);
        trigger_monitor(msg);
    }
    pthread_mutex_unlock(&pt_mutex);
}
void pm_exit(int pid, int status, int thread_id, bool is_kill) {
    pthread_mutex_lock(&pt_mutex);
    for (int i = 0; i < MAX_PROCESSES; i++) {
        if (process_table[i].pid == pid && process_table[i].state != STATE_TERMINATED) {
            process_table[i].state = STATE_ZOMBIE;
            process_table[i].exit_status = status;
            char msg[256];
            if (is_kill) {
                snprintf(msg, sizeof(msg), "Thread %d calls pm_kill %d", thread_id, pid);
            } else {
                snprintf(msg, sizeof(msg), "Thread %d calls pm_exit %d %d", thread_id, pid, status);
            }
            trigger_monitor(msg);
            pthread_cond_broadcast(&wait_cv);
            break;
        }
    }
    pthread_mutex_unlock(&pt_mutex);
}
void pm_wait(int parent_pid, int child_pid, int thread_id) {
    pthread_mutex_lock(&pt_mutex);
    int parent_idx = -1;
    for (int i = 0; i < MAX_PROCESSES; i++) {
        if (process_table[i].pid == parent_pid) {
            parent_idx = i;
            process_table[i].state = STATE_BLOCKED;
            break;
        }
    }
    bool child_reaped = false;
    while (!child_reaped) {
        for (int i = 0; i < MAX_PROCESSES; i++) {
            if (process_table[i].ppid == parent_pid &&
               (child_pid == -1 || process_table[i].pid == child_pid)) {
                if (process_table[i].state == STATE_ZOMBIE) {
                    process_table[i].state = STATE_TERMINATED;
                    child_reaped = true;
                    char msg[256];
                    snprintf(msg, sizeof(msg), "Thread %d calls pm_wait %d %d", thread_id, parent_pid, child_pid);
                    trigger_monitor(msg);
                    break;
                }
            }
        }
        if (!child_reaped) {
            pthread_cond_wait(&wait_cv, &pt_mutex);
        }
    }
    if (parent_idx != -1) {
        process_table[parent_idx].state = STATE_RUNNING;
    }
    pthread_mutex_unlock(&pt_mutex);
}

void pm_kill(int pid, int thread_id) {
    pm_exit(pid, 9, thread_id, true); 
}
void* monitor_thread(void* arg) {
    pthread_mutex_lock(&pt_mutex);
    pm_ps();

    while (1) {
        while (!monitor_update_flag) {
            pthread_cond_wait(&monitor_cv, &pt_mutex);
        }
        pm_ps();
        monitor_update_flag = false;
    }
    pthread_mutex_unlock(&pt_mutex);
    return NULL;
}
typedef struct {
    int thread_id;
    char filename[256];
} WorkerArgs;

void* worker_thread(void* arg) {
    WorkerArgs* wargs = (WorkerArgs*)arg;
    FILE* file = fopen(wargs->filename, "r");
    if (!file) {
        printf("Error: Cannot open script %s\n", wargs->filename);
        free(wargs);
        return NULL;
    }

    char cmd[16];
    while (fscanf(file, "%s", cmd) != EOF) {
        if (strcmp(cmd, "fork") == 0) {
            int parent_pid;
            fscanf(file, "%d", &parent_pid);
            pm_fork(parent_pid, wargs->thread_id);
        } 
        else if (strcmp(cmd, "exit") == 0) {
            int pid, status;
            fscanf(file, "%d %d", &pid, &status);
            pm_exit(pid, status, wargs->thread_id, false);
        } 
        else if (strcmp(cmd, "wait") == 0) {
            int parent_pid, child_pid;
            fscanf(file, "%d %d", &parent_pid, &child_pid);
            pm_wait(parent_pid, child_pid, wargs->thread_id);
        } 
        else if (strcmp(cmd, "kill") == 0) {
            int pid;
            fscanf(file, "%d", &pid);
            pm_kill(pid, wargs->thread_id);
        } 
        else if (strcmp(cmd, "sleep") == 0) {
            int ms;
            fscanf(file, "%d", &ms);
            usleep(ms * 1000);
        }
    }

    fclose(file);
    free(wargs);
    return NULL;
}
int main(int argc, char *argv[]) {
    if (argc < 2) {
        printf("Usage: ./pm_sim script1.txt script2.txt ...\n");
        return 1;
    }
    snapshot_file = fopen("snapshots.txt", "w");
    if (!snapshot_file) {
        printf("Failed to create snapshots.txt\n");
        return 1;
    }
    for (int i = 0; i < MAX_PROCESSES; i++) {
        process_table[i].pid = 0;
        process_table[i].state = STATE_TERMINATED;
    }
    process_table[0].pid = 1;
    process_table[0].ppid = 0;
    process_table[0].state = STATE_RUNNING;
    process_table[0].exit_status = 0;
    pthread_t monitor;
    pthread_create(&monitor, NULL, monitor_thread, NULL);
    usleep(10000); 
    int num_workers = argc - 1;
    pthread_t workers[num_workers];

    for (int i = 0; i < num_workers; i++) {
        WorkerArgs* wargs = malloc(sizeof(WorkerArgs));
        wargs->thread_id = i;
        strncpy(wargs->filename, argv[i + 1], sizeof(wargs->filename) - 1);
        pthread_create(&workers[i], NULL, worker_thread, wargs);
    }
    for (int i = 0; i < num_workers; i++) {
        pthread_join(workers[i], NULL);
    }
    usleep(50000); 

    fclose(snapshot_file);
    return 0;
}
