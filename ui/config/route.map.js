
        /**
         * Don't change the constante name as it'll impact on the component routing
         */
        
export const stillRoutesMap = {
    viewRoutes: {
        regular: {
            HomeComponent: {
                path: "app/home",
                url: "/HomeComponent"
            },
            Workspace: {
                path: "app/components/workspace",
                url: "/workspace"
            },
            ObjectType: {
                path: "app/components/workspace/object-type",
                url: "/object-type"
            },
            Bucket: {
                path: "app/components/node-types",
                url: "/bucket-type"
            },
            CleanerType: {
                path: "app/components/node-types",
                url: "/cleaner-type"
            },
            DuckDBOutput: {
                path: "app/components/node-types",
                url: "/duck-dbo-utput"
            },
            Terminal: {
                path: "app/components/workspace/terminal",
                url: "/terminal"
            },
            SqlDBComponent: {
                path: "app/components/node-types",
                url: "/sql-db"
            }
        },
        lazyInitial: {}
    }
}



