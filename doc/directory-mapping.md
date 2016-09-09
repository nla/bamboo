# Directory Mapping Rules

## Building the structure

* For each subject
  - Create a corresponding category with the same name
  - If the subject has a parent subject, link the new category to the corresponding parent category.
  
* For each collection
  - Create a corresponding category with the same name
  - Add curating agencies to the category based on the owner of each title in the collection.
  - If the collection has a parent collection, link the new category to the corresponding parent category.
  - For each subject the collection is in:
    + If there was a parent collection and it has this same subject, ignore this subject.
    + If the new category has no parent yet, set its parent to this subject.
    + Otherwise, since the category already has a parent, add a reference from the subject.
  
* For each title
  - If the title has issues
    + Create a category for the title
    + Create a subcategory for each issue group
    + Add each issue as an entry in the appropriate category
    + Ignore the instances
  - Else if the title has multiple instances with different domains
    + Create a category for the title
    + Create entries in the category for each variation
      - Name these entries using the domain and year ranges: "Jason's blog (geocities.com) 1998-2002", "Jason's blog (jason.id.au) 2003-"
  - Otherwise if the title has instances all sharing the same domain
    + Create a directory entry for the title
  - For each collection the title is in:
    + If the title has no parent, set the parent to the be collection.
    + Otherwise, since the title already has a parent add a reference from the collection.
  - For each subject the title is in that is in the "subjects we want to treat as a collection" list:
    + Apply the above collection rules.

## Individual fields

### Subject

| Directory field            | Source                             |
| -------------------------- | ---------------------------------- |
| category.name              | subject.subject_name               |
| category.description       | null                               |
| category.parent_id         | category.id WHERE category.pandas_subject_id = subject.subject_parent_id |
| category.pandas_subject_id | subject.subject_id                 |
| category_agency            | none                               |

### Collection

| Directory field               | Source                             |
| ----------------------------- | ---------------------------------- |
| category.name                 | col.name                           |
| category.description          | col.display_comment                |
| category.parent_id            | category.id WHERE category.pandas_collection_id = col.col_parent_id |
| category.pandas_collection_id | col.col_id                         |
| category_agency               | (based on title membership)        |

### Title (as category)

| Directory field               | Source    |
| ----------------------------- | --------- |
| category.name                 | title.name          |
| category.description          | null                |
| category.parent_id            | (first collection or subject) |
| category.pandas_title_id      | title.pi                      |
| category_agency               | title.agency_id      |

### Title (as entry)

| Directory field                | Source    |
| -----------------------------  | --------- |
| entry.name                     | title.name                  |
| entry.url                      | title.title_url             |
| entry.category_id              | (based on first collection) |

### Issue group

| Directory field                | Source    |
| -----------------------------  | --------- |
| category.name                  | issue_group.name          |
| category.description           | null                        |
| category.parent_id             | (owning title)              |
| category.display_order         | issue_group.display_order |
| category.pandas_issue_group_id | issue_group.issue_group_id  |
| category_agency                | title.agency_id      |

### Issue

| Directory field                | Source    |
| -----------------------------  | --------- |
| entry.name                     | arch_issue.title              |
| entry.url                      | arch_issue.url                |
| entry.display_order            | arch_issue.issue_order        |
| entry.category_id              | (owning issue group or title) |


