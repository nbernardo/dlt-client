export const mockData = {
    "1": {
        "content": "Parent 1",
        "childs": [
            {
                "content": "Child parent 1",
                "childs": [
                    {
                        "content": "Grand Child",
                        "childs": []
                    }
                ]
            }
        ],
        "isTopLevel": true
    },
    "2": {
        "content": "Second parent",
        "childs": [
            {
                "content": "Child parent 2",
                "childs": [
                    {
                        "content": "Grand child sec par",
                        "childs": []
                    },
                    {
                        "content": "2 Grand child sec par",
                        "childs": []
                    },
                    {
                        "content": "3 Grand child sec par",
                        "childs": []
                    },
                ]
            }
        ],
        "isTopLevel": true
    }
}