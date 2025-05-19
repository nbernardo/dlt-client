const mockData = {
    "noutro_teste.duckdb": {
        "content": "\n\t\t\t\t\t<div class=\"ppline-treeview\">\n\t\t\t\t\t\t<span> \n    <img \n        src=\"app/assets/imgs/pipeline.png\" \n        class=\"tbl-to-terminal\">\n noutro_teste.duckdb </span>\n\t\t\t\t\t\t<span style=\"display: none;\">\n    <i class='fas fa-plug' \n        style='\n            margin-left: -20px; \n            position:absolute; \n            z-index: 1000;\n            color: grey;\n        '>\n    </i>\n<span>\n\t\t\t\t\t</div>",
        "childs": [
            {
                "content": "\n\t\t\t\t\t<div class=\"table-in-treeview\">\n\t\t\t\t\t\t<span> \n    <i class='fas fa-database' \n        style=\"margin-left: -20px; position:absolute; z-index: 1000;\">\n    </i>\n <b>agora</b></span>\n\t\t\t\t\t\t<span onclick=\"$still.component.ref('_cmp1540208178251199').connectToDatabase(event, 'noutro_teste.duckdb')\">\n    <i class='fas fa-plug' \n        style='\n            margin-left: -20px; \n            position:absolute; \n            z-index: 1000;\n            color: grey;\n        '>\n    </i>\n</span>\n\t\t\t\t\t</div>\n\t\t\t\t",
                "childs": [
                    {
                        "content": "\n\t\t\t\t\t\t<div class=\"table-in-treeview\">\n\t\t\t\t\t\t\t<span>\n    <i class='fas fa-table' \n        style='margin-left: -20px; position:absolute; z-index: 1000'>\n    </i>\n primeira</span>\n\t\t\t\t\t\t\t<span \n\t\t\t\t\t\t\t\tonclick=\"$still.component.ref('_cmp1540208178251199').genInitialDBQuery('agora.primeira')\"\n\t\t\t\t\t\t\t\ttooltip-x=\"-130\" tooltip=\"Query primeira table\" \n\t\t\t\t\t\t\t\tclass=\"term-icn-container\"\n\t\t\t\t\t\t\t>\n\t\t\t\t\t\t\t\t\n    <img \n        src=\"app/assets/imgs/terminal-cli-fill.svg\" \n        class=\"tbl-to-terminal\">\n\n\t\t\t\t\t\t\t</span>\n\t\t\t\t\t\t</div>",
                        "childs": []
                    }
                ]
            }
        ],
        "isTopLevel": true,
        "id": "noutro_teste.duckdb"
    },
    "pipeline_name.duckdb": {
        "content": "\n\t\t\t\t\t<div class=\"ppline-treeview\">\n\t\t\t\t\t\t<span> \n    <img \n        src=\"app/assets/imgs/pipeline.png\" \n        class=\"tbl-to-terminal\">\n pipeline_name.duckdb </span>\n\t\t\t\t\t\t<span style=\"display: none;\">\n    <i class='fas fa-plug' \n        style='\n            margin-left: -20px; \n            position:absolute; \n            z-index: 1000;\n            color: grey;\n        '>\n    </i>\n<span>\n\t\t\t\t\t</div>",
        "childs": [
            {
                "content": "\n\t\t\t\t\t<div class=\"table-in-treeview\">\n\t\t\t\t\t\t<span> \n    <i class='fas fa-database' \n        style=\"margin-left: -20px; position:absolute; z-index: 1000;\">\n    </i>\n <b>newdb</b></span>\n\t\t\t\t\t\t<span onclick=\"$still.component.ref('_cmp1540208178251199').connectToDatabase(event, 'pipeline_name.duckdb')\">\n    <i class='fas fa-plug' \n        style='\n            margin-left: -20px; \n            position:absolute; \n            z-index: 1000;\n            color: grey;\n        '>\n    </i>\n</span>\n\t\t\t\t\t</div>\n\t\t\t\t",
                "childs": [
                    {
                        "content": "\n\t\t\t\t\t\t<div class=\"table-in-treeview\">\n\t\t\t\t\t\t\t<span>\n    <i class='fas fa-table' \n        style='margin-left: -20px; position:absolute; z-index: 1000'>\n    </i>\n employees</span>\n\t\t\t\t\t\t\t<span \n\t\t\t\t\t\t\t\tonclick=\"$still.component.ref('_cmp1540208178251199').genInitialDBQuery('newdb.employees')\"\n\t\t\t\t\t\t\t\ttooltip-x=\"-130\" tooltip=\"Query employees table\" \n\t\t\t\t\t\t\t\tclass=\"term-icn-container\"\n\t\t\t\t\t\t\t>\n\t\t\t\t\t\t\t\t\n    <img \n        src=\"app/assets/imgs/terminal-cli-fill.svg\" \n        class=\"tbl-to-terminal\">\n\n\t\t\t\t\t\t\t</span>\n\t\t\t\t\t\t</div>",
                        "childs": []
                    },
                    {
                        "content": "\n\t\t\t\t\t\t<div class=\"table-in-treeview\">\n\t\t\t\t\t\t\t<span>\n    <i class='fas fa-table' \n        style='margin-left: -20px; position:absolute; z-index: 1000'>\n    </i>\n products</span>\n\t\t\t\t\t\t\t<span \n\t\t\t\t\t\t\t\tonclick=\"$still.component.ref('_cmp1540208178251199').genInitialDBQuery('newdb.products')\"\n\t\t\t\t\t\t\t\ttooltip-x=\"-130\" tooltip=\"Query products table\" \n\t\t\t\t\t\t\t\tclass=\"term-icn-container\"\n\t\t\t\t\t\t\t>\n\t\t\t\t\t\t\t\t\n    <img \n        src=\"app/assets/imgs/terminal-cli-fill.svg\" \n        class=\"tbl-to-terminal\">\n\n\t\t\t\t\t\t\t</span>\n\t\t\t\t\t\t</div>",
                        "childs": []
                    }
                ]
            }
        ],
        "isTopLevel": true,
        "id": "pipeline_name.duckdb"
    }
}