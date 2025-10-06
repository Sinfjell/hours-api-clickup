# Project Reference — [Project Name]
_Last updated: YYYY-MM-DD_

## 🧩 Overview
Short summary describing the project purpose and problem solved.

## ⚙️ Installation & Setup
Steps for developers.

## 🔧 Hooks & Filters
Relevant hooks, filters, or integrations.

## 🧠 Key Functions
Core functions and return values.

## 🌐 Endpoints / Routes
REST or feed endpoints with methods and responses.

### REST API Endpoints
**Base URL:** `/wp-json/[namespace]/v[version]/`

| Endpoint | Method | Description | Parameters |
|----------|--------|-------------|------------|
| `/endpoint-path` | GET | [Description] | `param1` (string, required) |
| `/another-endpoint` | POST | [Description] | `param2` (integer, optional) |

### External Integrations
- **[Service Name]**: [Description] - [Authentication method]
- **[Another Service]**: [Description] - [Authentication method]

## 🗄️ Data & Options
Meta keys, options, constants, caching.

### Database Tables
| Table Name | Purpose | Key Fields |
|------------|---------|------------|
| `wp_plugin_table` | [Description] | `id`, `name`, `value` |

### Meta Fields
| Meta Key | Data Type | Description | Usage |
|----------|-----------|-------------|-------|
| `_plugin_field1` | string | [Description] | [Where it's used] |
| `_plugin_field2` | array | [Description] | [Where it's used] |

### Options
| Option Name | Data Type | Description | Default Value |
|-------------|-----------|-------------|---------------|
| `plugin_option1` | string | [Description] | [Default] |
| `plugin_option2` | array | [Description] | [Default] |

### Cache Keys
| Key Pattern | Purpose | Expiration |
|-------------|--------|------------|
| `plugin_cache_[id]` | [Description] | [Time] |

## 🪄 Design Notes
Implementation logic, decisions, limitations.

## 🧾 Changelog
Inline summary of key versions; link to [changelog.md](changelog.md) if needed.

---

## 📝 Note on Documentation Structure

This reference file contains all essential technical information. However, if your project is particularly heavy on one area, consider creating separate files:

- **`api.md`** - If you have many REST endpoints or complex external integrations
- **`data.md`** - If you have extensive database schemas or complex data relationships

Keep the main `reference.md` as your primary technical reference, and only split out when the content becomes unwieldy.

---

**Purpose:** Make it possible for another developer to understand the technical structure in under 2 minutes.
