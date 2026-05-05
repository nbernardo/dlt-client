export const pipelinePlanContent = {
  "content": {
    "Home": {
      "data": {
        "1": {
          "id": 1, "name": "Start", "data": {}, "class": "Start", "html": "<div class=\"edge-label\">Start</div><div></div>", "typenode": false, "inputs": {}, 
          "pos_x": 67, "pos_y": 223.16666666666666, "outputs": { "output_1": { "connections": [{ "node": "2", "output": "input_1" }] } }
        },
        "2": {
          "id": 2,
          "name": "SqlDBComponent",
          "data": {
            "componentId": "dynamic-_cmp12526392907717077SqlDBComponent", "host": "127.0.0.1", "connectionName": "New Oddo DB Local", "database": "newdb", 
            "dbengine": "postgresql", "namespace": "google-oauth2_116448449010041059402", "tables": {}, "primaryKeys": {}
          },
          "class": "SqlDBComponent", "html": "", "typenode": false, "inputs": { "input_1": { "connections": [ { "node": "1", "input": "output_1" } ] } },
          "outputs": { "output_1": { "connections": [ { "node": "3", "output": "input_1" } ] } }, "pos_x": 176, "pos_y": 97
        },
        "3": {
          "id": 3, "name": "DuckDBOutput", "html": "", "class": "DuckDBOutput", "typenode": false,
          "data": { "componentId": "dynamic-_cmp395789456079922DuckDBOutput", "database": "fleetdatamart" },
          "inputs": { "input_1": { "connections": [ { "node": "2", "input": "output_1" } ] } }, "outputs": {}, "pos_x": 505, "pos_y": 155
        }
      }
    }
  },
  "pipeline_lbl": "", "goldQuery": ""
}