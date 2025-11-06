### Secret Management Designe System & Overview - part of MVP2

**Status:** 🟢 Completed – Nov/6/2025

**Context:** In order to allow better and secure credentials management, Secret Management comes into place there by providing robust capabilities such as:
  
  - Automated and scheduled flows at scale, ensuring pipelines run smoothly
  - Intelligent orchestration at scale, allowing our AI agent to act securely whenever needed
  - Self and Independenc credentials management


<br>

#### System Design diagram overview

<img src="ui/app/assets/imgs/DLT-Client-Database & API Catalog.png">

Behind the scenes, Client DLT is leveraging Hashicorp Vault to handle/store the secrets there by offloading both storage and encryption handling.

<br>

**Frontend files/code considerations**

In terms of Frontend, things are implemented on the in the `CatalogForm` component, which is rendered by `WorkSpaceController`, and to send data to backend, it uses `WorkspaceService`.

```
├── ui/
│   └── app/
│       └── components/
│       |   └── catalog/
│       |       └── CatalogForm.js
│       |       └── CatalogForm.html
|       |
│       └── services/
│       |   └── WorkspaceService.js
│       └── controller/
│       |   └── WorkSpaceController.js
│       └── ...
│       └── ...
└── README.md
```


<br>

**Frontend files/code considerations**

In terms of Backend, the `workspace.py` is the entry point, which naturally routes to `SecretManager.py` which connects to the Hashicorp Vault.

```
├── backend/
│   └── src/
│       └── controller/
│       |   └── workspace.py
|       |
│       └── services/
│       |   └── workspace
│       |       └── SecretManager.py
|       |
│       └── utils/
│       |   └── database_secret.py
│       └── ...
│       └── ...
└── README.md
```

This feature part of the MVP2 release.