const mockData = {
    dd: {
        "nodeLabel": "newdb",
        "childs": [
            {
                "content": "<a href='#'>employees</a>",
                "dbSize": 23,
                "colCount": 10,
            },
            {
                "content": "products",
                "dbSize": 110,
                "colCount": 11,
                "childs": [
                    {
                        "nodeLabel": "Sub Label",
                        "content": "subtabl1",
                        "dbSize": 23,
                        "colCount": 10,
                        "childs": [
                            {
                                "content": "infirst table",
                                "dbSize": 23,
                                "colCount": 10
                            },
                            {
                                "content": "<b>infirst another<b>",
                                "dbSize": 110,
                                "colCount": 11
                            }
                        ]
                    },
                    {
                        "content": "secsubtbl",
                        "dbSize": 110,
                        "colCount": 11
                    }
                ]
            }
        ],
    }
}