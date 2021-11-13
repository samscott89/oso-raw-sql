from dataclasses import dataclass, fields
from typing import Any
from oso import Oso, Relation
import sqlite3
con = sqlite3.connect('test.db',detect_types=sqlite3.PARSE_COLNAMES)
con.row_factory = sqlite3.Row

cursor = con.cursor()
if False:
    cursor.executescript("""
CREATE TABLE orgs (
        id INTEGER NOT NULL,
        name VARCHAR,
        base_repo_role VARCHAR,
        billing_address VARCHAR,
        PRIMARY KEY (id),
        UNIQUE (name)
);
INSERT INTO orgs VALUES(1,'The Beatles','reader','64 Penny Ln Liverpool, UK');
INSERT INTO orgs VALUES(2,'Monsters Inc.','reader','123 Scarers Rd Monstropolis, USA');
CREATE TABLE users (
        id INTEGER NOT NULL,
        email VARCHAR,
        PRIMARY KEY (id),
        UNIQUE (email)
);
INSERT INTO users VALUES(1,'john@beatles.com');
INSERT INTO users VALUES(2,'paul@beatles.com');
INSERT INTO users VALUES(3,'admin@admin.com');
INSERT INTO users VALUES(4,'mike@monsters.com');
INSERT INTO users VALUES(5,'sully@monsters.com');
INSERT INTO users VALUES(6,'ringo@beatles.com');
INSERT INTO users VALUES(7,'randall@monsters.com');
CREATE TABLE org_roles (
        id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        org_id INTEGER NOT NULL,
        name VARCHAR,
        PRIMARY KEY (id),
        FOREIGN KEY(user_id) REFERENCES users (id),
        FOREIGN KEY(org_id) REFERENCES orgs (id)
);
INSERT INTO org_roles VALUES(1,1,1,'owner');
INSERT INTO org_roles VALUES(2,2,1,'member');
INSERT INTO org_roles VALUES(3,6,1,'member');
INSERT INTO org_roles VALUES(4,4,2,'owner');
INSERT INTO org_roles VALUES(5,5,2,'member');
INSERT INTO org_roles VALUES(6,7,2,'member');
    """)



class Subquery:
    def __init__(self, name: str, subquery):
        self.name = name
        self.subquery = subquery

    def as_query(self):
        clauses, params = self.subquery.as_where()
        return f"select {self.name} from {self.subquery.table} WHERE {clauses}", params

class Query:
    def __init__(self, table: str):
        self.table = table
        self.clauses = [[]]
        self.params = []

    def __iter__(self):
        return iter([self])

    def __getattr__(self, name: str) -> Any:
        # Constructs a subquery that only selects this one field
        # so it can be used in an `in` query
        return Subquery(name, self)

    def combine(self, other):
        if self.table != other.table:
            raise Exception("cannot union different tables together")
        self.clauses.extend(other.clauses)
        self.params.extend(other.params)
        return self
        

    def set(self, cond: str, value: Any) -> None:
        self.clauses[0].append(cond)
        if isinstance(value, list):
            self.params.extend(value)
        else:
            self.params.append(value)

    def as_select(self):
        clause, params = self.as_where()
        return f"select * from {self.table} where {clause}", params

    def as_where(self):
        return " OR ".join(" AND ".join(c) for c in self.clauses), self.params


def build_query(table, filters):
    handlers = {
        'Eq': lambda a, b: f'{a} = ?',
        'Neq': lambda a, b: f'{a} != ?',
        'In': lambda a, b: f'{a} IN ({", ".join(["?"] * len(b))})',
        'Nin': lambda a, b: f'{a} NOT ({", ".join(["?"] * len(b))})',
    }

    query = Query(table=table)
    for filter in filters:
        if filter.field is None:
            field = 'id' # default primary key
            value = filter.value.id

        elif isinstance(filter.field, list):
            field = [f'{fld}' for fld in filter.field]
            value = filter.value
        else:
            field = f'{filter.field}'
            value = filter.value

        if not isinstance(field, list):
            cond = handlers[filter.kind](field, value)
            if isinstance(value, list) and isinstance(value[0], Subquery):
                # flatten subquery, append params
                subq, params = value[0].as_query()
                cond = cond.replace('?', subq)
                query.clauses[0].append(cond)
                query.params.extend(params)
            else:
                query.set(cond, value)
        else:
            assert False, "unimplemented"

    return query

def exec_query(cls, query):
    return query

def combine_query(a, b):
    return a.combine(b)

oso = Oso()

@dataclass
class User:
    id: int
    email: int

@dataclass
class OrgRole:
    id: int
    user_id: int
    org_id: int
    name: str

@dataclass
class Org:
    id: int
    name: str
    base_repo_role: str
    billing_address: str

oso.register_class(
    OrgRole,
    fields={
        'user': Relation(
            kind='one',
            other_type='User',
            my_field='user_id',
            other_field='id'),
        'name': str,
        'user_id': int
    },
    build_query=lambda f: build_query('org_roles', f),
    exec_query=lambda q: exec_query(OrgRole, q),
    combine_query=combine_query)

oso.register_class(
    Org,
    fields={
        "roles": Relation(
            kind='many',
            other_type='OrgRole',
            my_field='id',
            other_field='org_id'),
        "name": str
    },
    build_query=lambda f: build_query('orgs', f),
    exec_query=lambda q: exec_query(Org, q),
    combine_query=combine_query,
)

oso.register_class(
    User,
    # fields={
    #     "id": int,
    # },
    build_query=lambda q: build_query('users', q),
    exec_query=lambda q: exec_query(User, q))

oso.load_files(['policy.polar'])


q = oso.authorized_query(User(id=1, email="john@beatles.com"), "read", Org)
# breakpoint()
print(q.as_select())

orgs = [Org(**d) for d in cursor.execute(*q.as_select())]
print(orgs)