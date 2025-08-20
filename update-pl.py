# lambda_function.py
import os, time, socket, ipaddress, uuid
import boto3
from botocore.exceptions import ClientError

ec2 = boto3.client("ec2")

PREFIX_LIST_ID = os.environ["PREFIX_LIST_ID"]       # pl-xxxxxxxx
FQDN_LIST      = os.environ["FQDN_LIST"]            # 改行 or カンマ区切りのFQDN群
MAX_ENTRIES    = 60                                  # 固定
DESC           = os.getenv("ENTRY_DESC", "managed")

def _fqdns_from_env():
    return [x.strip() for x in FQDN_LIST.replace(",", "\n").splitlines() if x.strip()]

def _resolve_ipv4_cidrs(fqdns):
    out = set()
    for h in fqdns:
        try:
            for fam, *_ , sa in socket.getaddrinfo(h, None):
                ip = sa[0]
                try:
                    if ipaddress.ip_address(ip).version == 4:
                        out.add(f"{ip}/32")
                except ValueError:
                    pass
        except Exception as e:
            print(f"[WARN] DNS resolve failed: {h} ({e})")
    return out

def _pl_meta(pl_id):
    pl = ec2.describe_managed_prefix_lists(PrefixListIds=[pl_id])["PrefixLists"][0]
    return pl["Version"], pl["MaxEntries"], pl["AddressFamily"], pl["State"]

def _current_cidrs(pl_id):
    s = set()
    paginator = ec2.get_paginator("get_managed_prefix_list_entries")
    for page in paginator.paginate(PrefixListId=pl_id):
        for e in page.get("Entries", []):
            s.add(e["Cidr"])
    return s

def handler(event, context):
    version, max_entries, af, state = _pl_meta(PREFIX_LIST_ID)
    if af != "IPv4":
        raise RuntimeError("This function expects an IPv4 prefix list.")

    desired = _resolve_ipv4_cidrs(_fqdns_from_env())
    if not desired:
        print("[INFO] No IPv4 resolved from FQDN_LIST; doing nothing.")
        return {"changed": False, "reason": "no_resolved_ipv4"}

    if len(desired) > MAX_ENTRIES:
        raise RuntimeError(f"Resolved {len(desired)} entries > MAX_ENTRIES({MAX_ENTRIES}). Reduce FQDNs.")

    current = _current_cidrs(PREFIX_LIST_ID)
    to_add = sorted(desired - current)
    to_del = sorted(current - desired)

    if not to_add and not to_del:
        print("[INFO] No change.")
        return {"changed": False, "entries": len(current)}

    print(f"[INFO] add={len(to_add)} del={len(to_del)} target_max={MAX_ENTRIES}")

    def _modify(cur_ver):
        return ec2.modify_managed_prefix_list(
            PrefixListId=PREFIX_LIST_ID,
            CurrentVersion=cur_ver,
            AddEntries=[{"Cidr": c, "Description": DESC} for c in to_add],
            RemoveEntries=[{"Cidr": c} for c in to_del],
            ClientToken=str(uuid.uuid4()),
        )

    # 1回だけリトライ（Versionズレ対策）
    try:
        resp = _modify(version)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("InvalidPrefixListVersion", "IncorrectState"):
            time.sleep(2)
            new_ver, *_ = _pl_meta(PREFIX_LIST_ID)
            resp = _modify(new_ver)
        else:
            raise

    return {
        "changed": True,
        "added": to_add,
        "removed": to_del,
        "new_version": resp.get("PrefixList", {}).get("Version")
    }
