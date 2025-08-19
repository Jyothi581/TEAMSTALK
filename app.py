import os
from flask import Flask, render_template, request, redirect, session, url_for, jsonify
from flask_socketio import SocketIO, emit
import MySQLdb, json
from datetime import datetime, timedelta
from kafka import KafkaProducer
from werkzeug.utils import secure_filename
from flask import flash
import time

online_users = set()
 
app = Flask(__name__)
app.secret_key = 'your_secret_key'
# Run without eventlet/gevent
socketio = SocketIO(app, cors_allowed_origins="*", manage_session=False, async_mode="threading")

# ────────────────────────────────────────────────────────────────────────────
# Socket registry (multi-session per user)
# ────────────────────────────────────────────────────────────────────────────
# username -> set(socket_ids)
user_sids: dict[str, set[str]] = {}
# socket_id -> username
sid_to_user: dict[str, str] = {}

def _register_sid(username: str, sid: str):
    if not username or not sid:
        return
    user_sids.setdefault(username, set()).add(sid)
    sid_to_user[sid] = username

def _unregister_sid(sid: str):
    username = sid_to_user.pop(sid, None)
    if not username:
        return
    bucket = user_sids.get(username)
    if bucket and sid in bucket:
        bucket.discard(sid)
        if not bucket:
            user_sids.pop(username, None)

def _emit_to_user(event: str, data, username: str):
    """Emit to all active sockets for a username (all tabs/browsers)."""
    for sid in user_sids.get(username, ()):
        socketio.emit(event, data, to=sid)

# ────────────────────────────────────────────────────────────────────────────

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_db():
    return MySQLdb.connect(
        host='127.0.0.1',
        user='root',
        passwd='Jyo7483##',
        db='teamtalk',
        charset='utf8'
    )

# ── INITIAL USER SEEDING ────────────────────────────────────────────────────
INITIAL_USERS = [
    ('Adarsh',   'Male'),
    ('Adithya',  'Male'),
    ('Aniket',   'Male'),
    ('Anusaya',  'Female'),
    ('Rohini',   'Female'),
    ('Rishika',  'Female'),
    ('Shashank', 'Male'),
    ('Shubha',   'Female'),
    ('Prateek',  'Male'),
    ('Shubham',  'Male'),
]
def seed_users():
    conn = get_db(); cur = conn.cursor()
    for username, gender in INITIAL_USERS:
        cur.execute(
            "INSERT IGNORE INTO users (username, gender) VALUES (%s, %s)",
            (username, gender)
        )
    conn.commit(); conn.close()
seed_users()

# ── AVATAR UPLOAD ──────────────────────────────────────────────────────────
AVATAR_FOLDER = os.path.join(app.static_folder, 'avatars')
os.makedirs(AVATAR_FOLDER, exist_ok=True)
ALLOWED_AVATAR_EXT = {'png','jpg','jpeg','gif'}
def allowed_file(fn, allowed):
    return '.' in fn and fn.rsplit('.',1)[1].lower() in allowed

@app.route('/upload_avatar', methods=['POST'])
def upload_avatar():
    f = request.files.get('avatar')
    if not f or not allowed_file(f.filename, ALLOWED_AVATAR_EXT):
        return redirect(request.referrer or url_for('index'))
    ext = f.filename.rsplit('.',1)[1].lower()
    fn = secure_filename(f"{session['username']}.{ext}")
    f.save(os.path.join(AVATAR_FOLDER, fn))
    return redirect(request.referrer or url_for('index'))

# ── FILE UPLOAD FOR CHAT ───────────────────────────────────────────────────
UPLOAD_FOLDER = os.path.join(app.static_folder, 'uploads')
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

@app.route('/upload_file', methods=['POST'])
def upload_file():
    f = request.files.get('file')
    if not f:
        return jsonify({'error':'no file'}), 400
    filename = secure_filename(f.filename)
    base, ext = os.path.splitext(filename)
    path = os.path.join(UPLOAD_FOLDER, filename)
    i = 1
    while os.path.exists(path):
        filename = f"{base}_{i}{ext}"
        path = os.path.join(UPLOAD_FOLDER, filename)
        i += 1
    f.save(path)
    url = url_for('static', filename=f"uploads/{filename}")
    return jsonify({'url': url})

# ── CHATROOMS ─────────────────────────────────────────────────────────────
CHATROOMS = [
    # "Data Science Daily DSM",
    # "Data Science Kafka Team",
    # "General Discussion Group"
]

# ── SIGNUP / LOGIN / LOGOUT ────────────────────────────────────────────────
@app.route('/signup', methods=['GET','POST'])
def signup():
    if request.method=='POST':
        u = request.form['username'].strip()
        g = request.form['gender']
        if not u:
            return render_template('signup.html', error="Username is required.")
        conn = get_db(); cur = conn.cursor()
        cur.execute("SELECT id FROM users WHERE username=%s", (u,))
        if not cur.fetchone():
            cur.execute("INSERT INTO users (username, gender) VALUES (%s,%s)", (u,g))
            conn.commit()
        conn.close()
        session['username']=u
        session['gender']=g
        return redirect(url_for('index'))
    return render_template('signup.html')

@app.route('/login', methods=['GET','POST'])
def login():
    if request.method=='POST':
        u = request.form['username'].strip()
        conn = get_db(); cur = conn.cursor()
        cur.execute("SELECT gender FROM users WHERE username=%s", (u,))
        row = cur.fetchone()
        conn.close()
        if row:
            session['username']=u
            session['gender']=row[0]
            return redirect(url_for('index'))
        else:
            return redirect(url_for('signup'))
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/')
def index():
    if 'username' not in session:
        return redirect(url_for('login'))

    me = session['username']
    active_room = request.args.get('chatroom')
    active_receiver = request.args.get('receiver')

    avatar_url = url_for('static', filename='avatars/default.png')
    for ext in ALLOWED_AVATAR_EXT:
        fn = f"{me}.{ext}"
        if os.path.exists(os.path.join(AVATAR_FOLDER, fn)):
            avatar_url = url_for('static', filename=f"avatars/{fn}")
            break

    conn = get_db()
    cur = conn.cursor()

    # get all users except me
    cur.execute("SELECT id, username, gender, last_seen FROM users WHERE username!=%s", (me,))
    users_raw = cur.fetchall()
    users = [{'id': r[0], 'name': r[1], 'gender': r[2], 'last_seen': r[3]} for r in users_raw]

    # get my user ID
    cur.execute("SELECT id FROM users WHERE username=%s", (me,))
    my_id = cur.fetchone()[0]

    # get all groups I am a member of
    cur.execute("""
        SELECT g.name FROM chat_groups g
        JOIN group_members gm ON g.id = gm.group_id
        WHERE gm.user_id = %s
    """, (my_id,))
    chatrooms = [r[0] for r in cur.fetchall()]

    # messages for current pane
    if active_receiver:
        if active_receiver == me:
            return redirect(url_for('index'))

        cur.execute("""
            UPDATE messages SET seen=TRUE
            WHERE sender=%s AND receiver=%s AND seen=FALSE
        """, (active_receiver, me))
        conn.commit()

        cur.execute("""
            SELECT sender, message, timestamp
            FROM messages
            WHERE (sender=%s AND receiver=%s)
               OR (sender=%s AND receiver=%s)
            ORDER BY timestamp
        """, (me, active_receiver, active_receiver, me))
        messages = cur.fetchall()
        room = None
    else:
        room = active_room or (chatrooms[0] if chatrooms else None)
        if chatrooms and room not in chatrooms:
            room = chatrooms[0]
        elif not chatrooms:
            room = None

        cur.execute("""
            SELECT sender, message, timestamp
            FROM messages
            WHERE chatroom=%s AND receiver IS NULL
            ORDER BY timestamp
        """, (room,))
        messages = cur.fetchall()
        active_room = room

    last_rooms, last_users, unread_counts = {}, {}, {}
    for r in chatrooms:
        cur.execute("SELECT timestamp FROM messages WHERE chatroom=%s AND receiver IS NULL ORDER BY timestamp DESC LIMIT 1", (r,))
        row = cur.fetchone()
        last_rooms[r] = row[0].strftime('%I:%M %p') if row else ''

    for u in users:
        name = u['name']
        cur.execute("""
            SELECT timestamp FROM messages
            WHERE (sender=%s AND receiver=%s) OR (sender=%s AND receiver=%s)
            ORDER BY timestamp DESC LIMIT 1
        """, (me, name, name, me))
        row = cur.fetchone()
        last_users[name] = row[0].strftime('%I:%M %p') if row else ''

        cur.execute("SELECT COUNT(*) FROM messages WHERE sender=%s AND receiver=%s AND seen=FALSE", (name, me))
        unread_counts[name] = cur.fetchone()[0]

    conn.close()

    return render_template('index.html',
        chatrooms=chatrooms,
        users=users,
        messages=messages,
        active_room=active_room,
        active_receiver=active_receiver,
        active_user=me,
        avatar_url=avatar_url,
        last_rooms=last_rooms,
        last_users=last_users,
        online_users=online_users,
        now=datetime.now,
        timedelta=timedelta,
        unread_counts=unread_counts
    )

# ── SOCKET.IO SEND / BROADCAST ────────────────────────────────────────────
@socketio.on('send_message')
def handle_send(data):
    s    = session['username']
    msg  = data.get('message')
    room = data.get('chatroom') or None
    recv = data.get('receiver') or None
    ts   = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    payload = {'sender': s, 'message': msg, 'timestamp': ts,
               'chatroom': room, 'receiver': recv}
    socketio.emit('receive_message', payload, skip_sid=request.sid)
    producer.send('teamtalk_chat', payload)
    producer.flush()

@app.route('/create_group', methods=['POST'])
def create_group():
    group_name = request.form.get('group_name', '').strip()
    members_json = request.form.get('members', '[]')

    try:
        member_ids = json.loads(members_json)
    except json.JSONDecodeError:
        flash("Invalid members format.", "danger")
        return redirect(url_for('index'))

    if not group_name or not member_ids:
        flash("Group name and members are required.", "danger")
        return redirect(url_for('index'))

    conn = get_db()
    cur = conn.cursor()

    # Get the ID of the current user
    username = session.get('username')
    if not username:
        flash("User session expired.", "danger")
        return redirect(url_for('login'))

    cur.execute("SELECT id FROM users WHERE username=%s", (username,))
    creator_row = cur.fetchone()
    if not creator_row:
        flash("Creator user not found.", "danger")
        return redirect(url_for('index'))
    creator_id = creator_row[0]

    # Add creator to members list if not already there
    if creator_id not in member_ids:
        member_ids.append(creator_id)

    # Check if group already exists
    cur.execute("SELECT id FROM chat_groups WHERE name=%s", (group_name,))
    if cur.fetchone():
        conn.close()
        flash("Group already exists.", "warning")
        return redirect(url_for('index'))

    # Create new group with admin_id
    cur.execute("INSERT INTO chat_groups (name, admin_id) VALUES (%s, %s)", (group_name, creator_id))
    conn.commit()

    # Get newly created group ID
    cur.execute("SELECT id FROM chat_groups WHERE name=%s", (group_name,))
    group_id = cur.fetchone()[0]

    # Insert members
    for user_id in member_ids:
        cur.execute("INSERT INTO group_members (group_id, user_id) VALUES (%s, %s)", (group_id, user_id))

    conn.commit()
    conn.close()

    flash(f"Group '{group_name}' created successfully!", "success")
    return redirect(url_for('index'))

def user_is_participant_or_creator(meeting_id, user_id):
    conn = get_db(); cur = conn.cursor()
    cur.execute("SELECT created_by FROM meetings WHERE id=%s", (meeting_id,))
    row = cur.fetchone()
    if not row:
        conn.close(); return False
    if row[0] == user_id:
        conn.close(); return True
    cur.execute("SELECT 1 FROM meeting_participants WHERE meeting_id=%s AND user_id=%s", (meeting_id, user_id))
    exists = cur.fetchone() is not None
    conn.close()
    return exists

@app.route('/calendar')
def calendar_view():
    if 'username' not in session:
        return redirect(url_for('login'))

    me = session['username']
    conn = get_db(); cur = conn.cursor()
    cur.execute("SELECT id, username, gender FROM users WHERE username!=%s", (me,))
    users_raw = cur.fetchall()
    users = [{'id': r[0], 'name': r[1], 'gender': r[2]} for r in users_raw]

    # get current user id
    cur.execute("SELECT id FROM users WHERE username=%s", (me,))
    my_id = cur.fetchone()[0]
    conn.close()

    return render_template('calendar.html', users=users, active_user=me, my_id=my_id)

@app.route('/api/meetings', methods=['GET'])
def list_meetings():
    if 'username' not in session:
        return jsonify([])

    me = session['username']
    conn = get_db(); cur = conn.cursor()
    cur.execute("SELECT id FROM users WHERE username=%s", (me,))
    user_id = cur.fetchone()[0]

    # fetch meetings where user is creator or participant
    cur.execute("""
      SELECT m.id, m.title, m.description, m.scheduled_at, m.end_time, m.call_link, m.created_by
      FROM meetings m
      LEFT JOIN meeting_participants mp ON m.id=mp.meeting_id
      WHERE m.created_by=%s OR mp.user_id=%s
      GROUP BY m.id
    """, (user_id, user_id))
    meetings = cur.fetchall()

    events = []
    for m in meetings:
        mid, title, description, start, end, call_link, created_by = m
        # fetch participant ids
        cur.execute("SELECT user_id FROM meeting_participants WHERE meeting_id=%s", (mid,))
        participants = [r[0] for r in cur.fetchall()]
        events.append({
            'id': mid,
            'title': title,
            'start': start.strftime('%Y-%m-%dT%H:%M:%S'),
            'end': end.strftime('%Y-%m-%dT%H:%M:%S') if end else None,
            'extendedProps': {
                'description': description,
                'participants': participants,
                'call_link': call_link,
                'created_by': created_by
            }
        })
    conn.close()
    return jsonify(events)

@app.route('/api/meetings', methods=['POST'])
def create_meeting():
    if 'username' not in session:
        return jsonify({'error':'unauthorized'}), 401
    data = request.json
    title = data.get('title','').strip()
    description = data.get('description','')
    start = data.get('start')  # ISO string
    end = data.get('end')      # ISO string or None
    participant_ids = data.get('participants', [])
    call_link = data.get('call_link', None)

    if not title or not start:
        return jsonify({'error':'title and start required'}), 400

    me = session['username']
    conn = get_db(); cur = conn.cursor()
    cur.execute("SELECT id FROM users WHERE username=%s", (me,))
    creator_id = cur.fetchone()[0]

    # insert meeting
    cur.execute("""
      INSERT INTO meetings (title, description, scheduled_at, end_time, created_by, call_link)
      VALUES (%s,%s,%s,%s,%s,%s)
    """, (title, description, start, end, creator_id, call_link))
    meeting_id = cur.lastrowid

    # ensure creator is participant
    participants = set(participant_ids)
    participants.add(creator_id)
    for uid in participants:
        cur.execute("INSERT INTO meeting_participants (meeting_id, user_id) VALUES (%s,%s)", (meeting_id, uid))
    conn.commit(); conn.close()
    return jsonify({'success': True, 'id': meeting_id})

@app.route('/api/meetings/<int:meeting_id>', methods=['PUT'])
def update_meeting(meeting_id):
    if 'username' not in session:
        return jsonify({'error':'unauthorized'}), 401
    me = session['username']
    conn = get_db(); cur = conn.cursor()
    cur.execute("SELECT id FROM users WHERE username=%s", (me,))
    user_id = cur.fetchone()[0]

    if not user_is_participant_or_creator(meeting_id, user_id):
        conn.close()
        return jsonify({'error':'forbidden'}), 403

    data = request.json
    title = data.get('title','').strip()
    description = data.get('description','')
    start = data.get('start')
    end = data.get('end')
    participant_ids = data.get('participants', [])
    call_link = data.get('call_link', None)

    # update meeting
    cur.execute("""
      UPDATE meetings SET title=%s, description=%s, scheduled_at=%s, end_time=%s, call_link=%s
      WHERE id=%s
    """, (title, description, start, end, call_link, meeting_id))

    # replace participants
    cur.execute("DELETE FROM meeting_participants WHERE meeting_id=%s", (meeting_id,))
    participants = set(participant_ids)
    participants.add(user_id)  # ensure updater remains
    for uid in participants:
        cur.execute("INSERT INTO meeting_participants (meeting_id, user_id) VALUES (%s,%s)", (meeting_id, uid))

    conn.commit(); conn.close()
    return jsonify({'success': True})

@app.route('/api/meetings/<int:meeting_id>', methods=['DELETE'])
def delete_meeting(meeting_id):
    if 'username' not in session:
        return jsonify({'error': 'unauthorized'}), 401

    me = session['username']
    conn = get_db(); cur = conn.cursor()
    cur.execute("SELECT id FROM users WHERE username=%s", (me,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return jsonify({'error': 'user not found'}), 404
    user_id = row[0]

    if not user_is_participant_or_creator(meeting_id, user_id):
        conn.close()
        return jsonify({'error': 'forbidden'}), 403

    cur.execute("DELETE FROM meetings WHERE id=%s", (meeting_id,))
    conn.commit()
    conn.close()
    return jsonify({'success': True})

@app.route('/chatroom_members/<chatroom>')
def get_chatroom_members(chatroom):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT id, username, gender FROM users")
    users = [{'id': u[0], 'name': u[1], 'gender': u[2]} for u in cur.fetchall()]

    cur.execute("""
        SELECT user_id FROM group_members 
        JOIN chat_groups ON group_members.group_id = chat_groups.id
        WHERE chat_groups.name = %s
    """, (chatroom,))
    members = [m[0] for m in cur.fetchall()]

    conn.close()
    return jsonify(users=users, members=members)

@app.route('/group_member_action', methods=['POST'])
def group_member_action():
    data = request.get_json()
    user_id = data['user_id']
    chatroom = data['chatroom']
    action = data['action']

    conn = get_db()
    cur = conn.cursor()

    # Get group ID by group name
    cur.execute("SELECT id FROM chat_groups WHERE name = %s", (chatroom,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return jsonify({'error': 'Group not found'}), 404

    group_id = row[0]

    if action == 'add':
        cur.execute("INSERT IGNORE INTO group_members (group_id, user_id) VALUES (%s, %s)", (group_id, user_id))
    elif action == 'remove':
        cur.execute("DELETE FROM group_members WHERE group_id = %s AND user_id = %s", (group_id, user_id))

    conn.commit()
    conn.close()
    return '', 204

# ── PRESENCE & REGISTRATION (multi-tab support) ────────────────────────────
@socketio.on('connect')
def handle_socket_connect():
    user = session.get('username')
    if user:
        _register_sid(user, request.sid)
        online_users.add(user)
        emit('user_status', {'user': user, 'status': 'online'}, broadcast=True)

@socketio.on('disconnect')
def handle_socket_disconnect():
    sid = request.sid
    user = sid_to_user.get(sid)
    _unregister_sid(sid)
    if user and user not in user_sids:  # no more sessions for this user
        online_users.discard(user)
        conn = get_db(); cur = conn.cursor()
        cur.execute("UPDATE users SET last_seen=%s WHERE username=%s", (datetime.now(), user))
        conn.commit(); conn.close()
        emit('user_status', {'user': user, 'status': 'offline'}, broadcast=True)

# Optional explicit registration (front-end can emit after io() connects)
@socketio.on('register_client')
def register_client(data):
    username = None
    if isinstance(data, dict):
        username = data.get('user') or data.get('username')
    if not username:
        username = session.get('username')
    _register_sid(username, request.sid)

# ── AUDIO CALLING ──────────────────────────────────────────────────────────
@socketio.on("call-user")
def handle_call_user(data):
    from_user = session.get('username')
    to_user = data.get("to")
    offer = data.get("offer")
    if not to_user or not offer:
        return
    _emit_to_user("incoming-call", {"from": from_user, "offer": offer}, to_user)

@socketio.on("answer-call")
def handle_answer_call(data):
    to_user = data.get("to")
    answer = data.get("answer")
    if not to_user or not answer:
        return
    _emit_to_user("call-answered", answer, to_user)
    # keep your timer behavior (both sides)
    _emit_to_user("start-timer", {}, to_user)
    caller = session.get('username')
    if caller:
        _emit_to_user("start-timer", {}, caller)

@socketio.on("start-timer")
def handle_start_timer(data):
    to_user = data.get("to")
    if to_user:
        _emit_to_user("start-timer", {}, to_user)

@socketio.on("ice-candidate")
def handle_ice_candidate(data):
    to_user = data.get("to")
    candidate = data.get("candidate")
    if not to_user or not candidate:
        return
    _emit_to_user("ice-candidate", {"candidate": candidate}, to_user)

@socketio.on("hangup")
def handle_hangup(data):
    to_user = data.get("to")
    if not to_user:
        return
    _emit_to_user("call-ended", {}, to_user)

@socketio.on("reject-call")
def handle_reject_call(data):
    to_user = data.get("to")
    if not to_user:
        return
    _emit_to_user("call-rejected", {}, to_user)

# ── VIDEO CALLING (mirrors audio) ──────────────────────────────────────────
@socketio.on('call-user-video')
def handle_call_user_video(data):
    from_user = session.get('username')
    to_user = data.get('to')
    offer = data.get('offer')
    if not to_user or not offer:
        return
    _emit_to_user('incoming-video-call', {'from': from_user, 'offer': offer}, to_user)

@socketio.on('answer-video-call')
def handle_answer_video_call(data):
    to_user = data.get('to')
    answer = data.get('answer')
    if not to_user or not answer:
        return
    _emit_to_user('video-call-answered', answer, to_user)

@socketio.on('video-ice-candidate')
def handle_video_ice_candidate(data):
    to_user = data.get('to')
    candidate = data.get('candidate')
    if not to_user or not candidate:
        return
    _emit_to_user('video-ice-candidate', {'candidate': candidate}, to_user)

@socketio.on('hangup-video')
def handle_hangup_video(data):
    to_user = data.get('to')
    if not to_user:
        return
    _emit_to_user('video-call-ended', {}, to_user)

@socketio.on('reject-video-call')
def handle_reject_video_call(data):
    to_user = data.get('to')
    if not to_user:
        return
    _emit_to_user('video-call-rejected', {}, to_user)

# ────────────────────────────────────────────────────────────────────────────

if __name__=='__main__':
    socketio.run(app, debug=True)
