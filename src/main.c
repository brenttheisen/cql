#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include "cql.h"

char *DEFAULT_HOST = "localhost:9042";

typedef struct {
  char **hosts;
  unsigned short hosts_count;
  char *keyspace;
} options;

void execute_file(int fd);
void execute_query(char *query);
void display_prompt();
options* parse_options(int argc, char *argv[]);
void print_rows_result(cql_rows_result *rows_result);
void print_rows_result_line(int column_count, int *column_max_chars, char **column_values);
char * format_column_value(cql_column_value *column_value, cql_column *column);

cql_cluster *cluster;
cql_session *session;


int main(int argc, char *argv[]) {
  options *opts = parse_options(argc, argv);

  cql_client_error *error;
  int res = cql_cluster_create(&cluster);
  if(res != CQL_RESULT_SUCCESS) {
    fprintf(stderr, "Error creating cluster");
    exit(EXIT_FAILURE);
  }

  int i;
  for(i = 0; i < opts->hosts_count; i++) {
    cql_host *host;
    if(cql_host_create(cluster, &host, opts->hosts[i], (void**) &error) == CQL_RESULT_CLIENT_ERROR) {
      fprintf(stderr, "Client error creating host for %s: %s", opts->hosts[i], error->message);
      cql_client_error_destroy(error);
      exit(EXIT_FAILURE);
    }
  }

  res = cql_session_create(cluster, &session, (void**) &error);
  if(res == CQL_RESULT_CLIENT_ERROR) {
    fprintf(stderr, "Client error creating session: %s", error->message);
    cql_client_error_destroy(error);
    exit(EXIT_FAILURE);
  }

  if(opts->keyspace) {
    char use_query[strlen(opts->keyspace) + 5];
    sprintf(use_query, "use %s", opts->keyspace);
    if(cql_session_query(session, use_query, CQL_CONSISTENCY_ANY, NULL) != CQL_RESULT_SUCCESS) {
      fprintf(stderr, "Error sending query to cluster: %s", use_query);
      exit(EXIT_FAILURE);
    }
  }

  if(isatty(fileno(stdin)))
    display_prompt();
  else
    execute_file(fileno(stdin));

  return 0;
}

options* parse_options(int argc, char *argv[]) {
  options *opts = malloc(sizeof(options));
  memset(opts, 0, sizeof(options));

  int opt;
  while((opt = getopt(argc, argv, "h:")) != -1) {
    switch(opt) {
    case 'h':
      {
        char **new_hosts = malloc(sizeof(char *) * (opts->hosts_count + 1));
        int i;
        for(i = 0; i < opts->hosts_count; i++) {
          new_hosts[i] = opts->hosts[i];
        }
        new_hosts[opts->hosts_count] = malloc(strlen(optarg) + 1);
        strcpy(new_hosts[opts->hosts_count], optarg);

        free(opts->hosts);
        opts->hosts = new_hosts;
        opts->hosts_count++;
      }
      break;
    default:
      fprintf(stderr, "Usage: %s [-h host[:port],host[:port]] [keyspace]\n", argv[0]);
      exit(EXIT_FAILURE);
    }
  }

  // TODO Set keyspace

  if(!opts->hosts_count) {
    opts->hosts = malloc(sizeof(char*));
    opts->hosts[0] = malloc(strlen(DEFAULT_HOST) + 1);
    strcpy(opts->hosts[0], DEFAULT_HOST);
    opts->hosts_count = 1;
  }

  return opts;
}

void execute_file(int fd) {
  const BUFFER_SIZE = 1024;
  char buffer[BUFFER_SIZE];
  char *carry_over = NULL;
  int bytes_read;
  while(bytes_read = read(fd, buffer, BUFFER_SIZE - 1)) {
    buffer[bytes_read] = 0;
    char *start = buffer;
    char *eos;
    while(eos = strstr(start, ";")) {
      int buffer_range_size = eos - start + 1;
      int len = buffer_range_size + 1;
      if(carry_over)
        len += strlen(carry_over);
      char *statement = malloc(len);
      char *statement_offset = statement;
      if(carry_over) {
        strcpy(statement, carry_over);
        statement_offset += strlen(carry_over);
        free(carry_over);
        carry_over = NULL;
      }
      strncpy(statement_offset, start, buffer_range_size);
      statement[len - 1] = 0;
      start = eos + 1;

      execute_query(statement);
    }

    int len = strlen(start) + 1;
    if(carry_over)
      len += strlen(carry_over);
    char *new_carry_over = malloc(len);
    if(carry_over)
      strcpy(new_carry_over, carry_over);
    strcpy(new_carry_over, start);
    if(carry_over)
      free(carry_over);
    carry_over = new_carry_over;
  }
}

void display_prompt() {
  printf("Prompt isn't implemented yet. Try redirecting CQL statements to stdin.");
}

void execute_query(char *query) {
  void *result;

  clock_t start = clock(), diff;
  // TODO The consistency level here needs to be configurable
  int res = cql_session_query(session, query, CQL_CONSISTENCY_ONE, &result);
  diff = clock() - start;

  // printf("QUERY\n-----------------\n%s\n-----------------\n", query);

  switch(res) {
  case CQL_RESULT_SUCCESS:
    {
      cql_result *res = (cql_result*) result;

      switch(res->kind) {
      case CQL_RESULT_KIND_ROWS:
        {
          cql_rows_result *rows_result = res->data;
          if(rows_result->rows_count > 0)
            print_rows_result(rows_result);
          printf("Query returned %d rows", rows_result->rows_count);
        }
        break;
      case CQL_RESULT_KIND_SET_KEYSPACE:
        printf("Set keyspace to %s", (char*) res->data);
        break;
      case CQL_RESULT_KIND_SCHEMA_CHANGE:
        {
          // TODO Output all the different variations correctly
          cql_schema_change *sc = (cql_schema_change*) res->data;
          printf("Schema change: %s on %s.%s", sc->change, sc->keyspace, sc->table);
        }
        break;
      }
      cql_result_destroy(res);
    }
    break;
  case CQL_RESULT_SERVER_ERROR:
    {
      cql_server_error *error = (cql_server_error*) result;
      fprintf(stderr, "Server error %lu: %s", error->code, error->message);
      cql_server_error_destroy(error);
    }
    break;
  case CQL_RESULT_CLIENT_ERROR:
    {
      cql_client_error *error = (cql_client_error*) result;
      printf("Client error: %s", error->message);
      cql_client_error_destroy(error);
    }
    break;
  default:
    fprintf(stderr, "Unknown result code return: %d\n", res);
    break;
  };

  printf("\nQuery executed in %.2f secs\n\n", (double) diff / 1000);
}

void print_rows_result(cql_rows_result *rows_result) {
  cql_rows_metadata *metadata = rows_result->metadata;
  int column_index;
  char *header_column_names[metadata->columns_count];
  int column_max_chars[metadata->columns_count];
  for(column_index = 0; column_index < metadata->columns_count; column_index++) {
    cql_column *column = metadata->columns[column_index];
    header_column_names[column_index] = column->column_name;
    column_max_chars[column_index] = strlen(header_column_names[column_index]);
  }

  int row_index;
  char ***formatted_column_values = malloc(sizeof(char **) * rows_result->rows_count);
  for(row_index = 0; row_index < rows_result->rows_count; row_index++) {
    cql_column_value **row = rows_result->rows[row_index];
    formatted_column_values[row_index] = malloc(sizeof(char *) * metadata->columns_count);

    for(column_index = 0; column_index < metadata->columns_count; column_index++) {
      cql_column_value *column_value = row[column_index];
      cql_column *column = metadata->columns[column_index];

      char *formatted_column_value = format_column_value(column_value, column);
      formatted_column_values[row_index][column_index] = formatted_column_value;

      int formatted_column_value_len = strlen(formatted_column_value);
      if(formatted_column_value_len > column_max_chars[column_index])
        column_max_chars[column_index] = formatted_column_value_len;
    }
  }

  print_rows_result_line(metadata->columns_count, column_max_chars, NULL);
  print_rows_result_line(metadata->columns_count, column_max_chars, header_column_names);
  print_rows_result_line(metadata->columns_count, column_max_chars, NULL);
  for(row_index = 0; row_index < rows_result->rows_count; row_index++) {
    print_rows_result_line(metadata->columns_count, column_max_chars, formatted_column_values[row_index]);
  }
  print_rows_result_line(metadata->columns_count, column_max_chars, NULL);

  for(row_index = 0; row_index < rows_result->rows_count; row_index++) {
    for(column_index = 0; column_index < metadata->columns_count; column_index++) {
      free(formatted_column_values[row_index][column_index]);
    }

    free(formatted_column_values[row_index]);
  }
  free(formatted_column_values);
}

void print_rows_result_line(int column_count, int *column_max_chars, char **column_values) {
  int i;
  for(i = 0; i < column_count; i++) {
    int max_chars = column_max_chars[i];

    printf(column_values ? "|" : "+");

    if(column_values) {
      printf(" %s", column_values[i]);
      int n, pad_right = max_chars + 1 - strlen(column_values[i]);
      for(n = 0; n < pad_right; n++)
        printf(" ");
    } else {
      int n;
      for(n = 0; n < max_chars + 2; n++)
        printf("-");
    }
  }

  printf(column_values ? "|\n" : "+\n");
}

char * format_column_value(cql_column_value *column_value, cql_column *column) {
  const char* UNKNOWN = "<UNKNOWN TYPE!>";
  const char* NULL_VALUE = "NULL";
  char *fmtval = NULL;

  if(!column_value) {
    fmtval = malloc(strlen(NULL_VALUE) + 1);
    strcpy(fmtval, NULL_VALUE);
  } else {
    switch(column->type) {
    case CQL_COLUMN_TYPE_INT:
      {
        int len = snprintf(NULL, 0, "%d", column_value->value);
        fmtval = malloc(len + 1);
        snprintf(fmtval, len + 1, "%d", column_value->value);
      }
      break;
    case CQL_COLUMN_TYPE_TEXT:
    case CQL_COLUMN_TYPE_VARCHAR:
      if(column_value->value) {
        fmtval = column_value->value;
      } else {
        fmtval = malloc(strlen(NULL_VALUE) + 1);
        strcpy(fmtval, NULL_VALUE);
      }
      break;
    default:
      fmtval = malloc(column_value->length * 2 + 3);
      strcpy(fmtval, "0x");
      int i;
      for(i = 0; i < column_value->length; i++)
        sprintf(fmtval + strlen(fmtval), "%02x", ((unsigned char*) column_value->value)[i]);
      break;
    }
  }

  return fmtval;
}
