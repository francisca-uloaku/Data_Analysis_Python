# from models import load_items
import json

'''
application = load_items("application")
# result = application[0].data


read_file = open("/Users/macbook/Desktop/Kippa/data-platform-reporting-server/apps/bizreg-staging/Paystack.json", 'r')
data = json.load(read_file)
user_details = data.get("data")

# value = [a for a in user_details if a.get("email") in [b.data.get("email") for b in application]]

# print(value)

paystack_data ={i.get("email"):i for i in user_details}


# print(paystack_data)
result = {}
for i, j in paystack_data.items():
    for b in application:
        if i == b.data.get("email"):
            b.data['ps_data'] = j
            result[i] = b.data
        if i not in [b.data.get("email") for b in application]:
            result[b.data.get("email")] = b.data
        

with open("sample.json", "w") as outfile:
    json.dump(result, outfile)


da_ = open("sample.json", "r")
dat_ = json.load(da_)
value = [d.get('ps_data') for d in dat_]
print(value)
 
# owner
# data
# received_date
# submitted
# submission_date
# processed_by
# artifacts
# stage '''

NAVBAR = (
    lambda stats: f"""
<ul class="nav nav-pills flex-column mb-sm-auto mb-0 align-items-center align-items-sm-start" id="menu">
    <li class="nav-item">
        <a href="#" class="nav-link align-middle px-0" style="color: yellow; font-size: large;">
            <i class="fs-4 bi-house"></i> <span class="ms-1 d-none d-sm-inline" onclick="javascript:hotReloadSection('Initial Review')">📁 Initial Review ({stats.get('UNPROCESSED_COUNT')})</span>
        </a>
    </li>
    <li class="nav-item">
        <a href="#" class="nav-link align-middle px-0" style="color: yellow; font-size: large;">
            <i class="fs-4 bi-house"></i> <span class="ms-1 d-none d-sm-inline" onclick="javascript:hotReloadSection('Awaiting Submission')">📬 Awaiting Submission ({stats.get('AWAITING_SUBMISSION_COUNT')})</span>
        </a>
    </li>
    <li class="nav-item">
        <a href="#" class="nav-link align-middle px-0" style="color: yellow; font-size: large;">
            <i class="fs-4 bi-house"></i> <span class="ms-1 d-none d-sm-inline" onclick="javascript:hotReloadSection('Awaiting Approval')">📬 Awaiting Approval ({stats.get('AWAITING_APPROVAL_COUNT')})</span>
        </a>
    </li>
    <li class="nav-item">
        <a href="#" class="nav-link align-middle px-0" style="color: yellow; font-size: large;">
            <i class="fs-4 bi-house"></i> <span class="ms-1 d-none d-sm-inline" onclick="javascript:hotReloadSection('Queried')">❓ Queried ({stats.get('QUERIED_COUNT')})</span>
        </a>
    </li>
    <li class="nav-item">
        <a href="#" class="nav-link align-middle px-0" style="color: yellow; font-size: large;">
            <i class="fs-4 bi-house"></i> <span class="ms-1 d-none d-sm-inline" onclick="javascript:hotReloadSection('Refund Requested')">📁 Refund Requested ({stats.get('REFUNDED_COUNT')})</span>
        </a>
    </li>
    <li class="nav-item">
        <a href="#" class="nav-link align-middle px-0" style="color: yellow; font-size: large;">
            <i class="fs-4 bi-house"></i> <span class="ms-1 d-none d-sm-inline" onclick="javascript:hotReloadSection('Cancelled')">📁 Cancelled ({stats.get('DELETED_COUNT')})</span>
        </a>
    </li>
    <li class="nav-item">
        <a href="#" class="nav-link align-middle px-0" style="color: yellow; font-size: large;">
            <i class="fs-4 bi-house"></i> <span class="ms-1 d-none d-sm-inline" onclick="javascript:hotReloadSection('Completed')">🚚 Completed ({stats.get('SUBMITTED_COUNT')})</span>
        </a>
    </li>
    <li class="nav-item">
        <a href="#" class="nav-link align-middle px-0" style="color: yellow; font-size: large;">
            <i class="fs-4 bi-house"></i> <span class="ms-1 d-none d-sm-inline" onclick="javascript:hotReloadSection('Summary View')">📊 Summary View ({stats.get('SUMMARY_VIEW_COUNT')})</span>
        </a>
    </li>
</ul>
"""
)

icon_stage = ["📁 Initial Review", 
    "📬 Awaiting Submission", 
    "📬 Awaiting Approval",
    "❓ Queried",
    "📁 Refunded",
    "📁 Deleted",
    "🚚 Submitted",
    "📊 Summary View"]


stats = {
            'INITIAL_REVIEW_COUNT': 1,
            'AWAITING_SUBMISSION_COUNT': 2,
            'AWAITING_APPROVAL_COUNT': 3,
            'REFUNDED_COUNT': 4,
            'DELETED_COUNT': 5,
            'QUERIED_COUNT': 6,
            "SUBMITTED_COUNT": 7,
            'SUMMARY_VIEW_COUNT' : 8}


def build_navbar(stats, icon_stage):

    nav_stuff = []

    icon_stage_list = [i.split(" ", 1)[1] for i in icon_stage]

    count_len = ['UNPROCESSED_COUNT',
            'AWAITING_SUBMISSION_COUNT',
            'AWAITING_APPROVAL_COUNT',
            'QUERIED_COUNT',
            'REFUNDED_COUNT',
            'DELETED_COUNT',
            'SUBMITTED_COUNT',
            'SUMMARY_VIEW_COUNT']
    
    stage_map = dict(zip(icon_stage_list, count_len))
    
    print(icon_stage_list)


    for i in icon_stage:
        split_view = i.split(" ", 1)
        stage = split_view[1]

        one_bar = f"""
        <li class="nav-item">
            <a href="#" class="nav-link align-middle px-0" style="color: yellow; font-size: large;">
                <i class="fs-4 bi-house"></i> <span class="ms-1 d-none d-sm-inline" onclick="javascript:hotReloadSection('{stage}')">{i} ({stats.get(stage_map.get(stage))})</span>
            </a>
        </li>
        """

        nav_stuff.append(one_bar)

    other_bar = "".join(nav_stuff)

    navbar = f"""
    <ul class="nav nav-pills flex-column mb-sm-auto mb-0 align-items-center align-items-sm-start" id="menu">
    {other_bar}
    </ul>
    """
    return navbar

NAVBAR_2 = (
    lambda stats, icon_stage: f"""{build_navbar(stats,icon_stage)}
    
"""
)

print(NAVBAR(stats))

print(NAVBAR_2(stats, icon_stage))