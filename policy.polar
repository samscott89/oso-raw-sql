actor User {
}

resource Org {
    relations = {roles: OrgRole};
    permissions = ["read", "join", "delete"];
    roles = ["owner", "member", "guest"];

    "read" if "member";
    "join" if "member";
    "read" if "guest";
    "join" if "guest";
    "delete" if "owner";

    "member" if "owner";
}

resource OrgRole {
    relations = {user: User};
}

allow(actor, action, resource) if has_permission(actor, action, resource);
# allow(_, "read", o: Org) if
#     o.name != "undiscoverable";

allow(_, "join", o: Org) if
    o.name = "public";

has_role(u: User, role_name: String, o: Org) if
    r in o.roles and
    r.name = role_name and
    r.user_id = u.id;