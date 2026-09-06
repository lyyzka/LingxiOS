import assert from 'node:assert/strict'
import http from 'node:http'
import { readFile } from 'node:fs/promises'
import { resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import ts from 'typescript'
import { PGlite } from '@electric-sql/pglite'

const source = resolve(process.argv[2] ?? process.env.LINGXILOOP_SOURCE ?? fileURLToPath(new URL('../../LingxiLoop/server/src', import.meta.url)))
const input = await readFile(resolve(source, 'modules/learning/teacher-reporting-repository.ts'), 'utf8')
const output = ts.transpileModule(input, { compilerOptions: { target: ts.ScriptTarget.ES2023, module: ts.ModuleKind.ESNext } }).outputText
const repository = await import(`data:text/javascript;base64,${Buffer.from(output).toString('base64')}`)
const db = new PGlite()
try {
  // Minimal query fixtures; this does not validate the full product schema or authorization.
  await db.exec(`
    CREATE TABLE learning_knowledge_units(id text PRIMARY KEY,company_id text,project_id text,position int);
    CREATE TABLE learning_knowledge_unit_dependencies(company_id text,project_id text,knowledge_unit_id text,prerequisite_knowledge_unit_id text);
    CREATE TABLE learning_activities(id text PRIMARY KEY,company_id text,project_id text,created_at timestamptz);
    CREATE TABLE learning_attempts(id text PRIMARY KEY,company_id text,project_id text,learner_id text,activity_id text);
    CREATE TABLE learning_evaluations(id text PRIMARY KEY,company_id text,project_id text,attempt_id text,status text,created_at timestamptz);
    INSERT INTO learning_knowledge_units VALUES('unit','tenant','project',1),('other-unit','other','project',2),('other-project','tenant','other',3);
    INSERT INTO learning_knowledge_unit_dependencies VALUES('tenant','project','unit','prerequisite'),('other','project','unit','foreign'),('tenant','other','unit','foreign-project');
    INSERT INTO learning_activities VALUES('activity','tenant','project',NOW()),('foreign','other','project',NOW()),('foreign-project','tenant','other',NOW());
    INSERT INTO learning_attempts VALUES('attempt','tenant','project','learner','activity'),('foreign','other','project','other-learner','foreign');
    INSERT INTO learning_evaluations VALUES('pending','tenant','project','attempt','PENDING',NOW()),('accepted','tenant','project','attempt','ACCEPTED',NOW()),('wrong-scope','other','project','attempt','PENDING',NOW()),('wrong-attempt','tenant','project','foreign','PENDING',NOW());
  `)
  const scope = { companyId: 'tenant', projectId: 'project', courseId: 'course' }
  assert.deepEqual(await repository.listTeacherObjectives(db, scope), [{ id: 'unit', company_id: 'tenant', project_id: 'project', position: 1, prerequisite_ids: ['prerequisite'] }])
  assert.deepEqual((await repository.listTeacherActivities(db, scope)).map(row => row.id), ['activity'])
  const reviews = await repository.listTeacherReviews(db, scope)
  assert.deepEqual(reviews.map(({ id, learner_id, activity_id }) => ({ id, learner_id, activity_id })), [{ id: 'pending', learner_id: 'learner', activity_id: 'activity' }])
  for (const method of ['listTeacherObjectives', 'listTeacherActivities', 'listTeacherReviews']) {
    assert.deepEqual(await repository[method](db, { ...scope, companyId: 'absent' }), [])
    assert.deepEqual(await repository[method](db, { ...scope, projectId: 'absent' }), [])
  }
  await db.exec(`
    CREATE TABLE users(id text PRIMARY KEY,display_name text,email text);
    CREATE TABLE project_memberships(company_id text,project_id text,user_id text,status text,role text);
    INSERT INTO users VALUES('learner','Learner','learner@example.invalid'),('observer','Observer','observer@example.invalid'),('inactive','Inactive','inactive@example.invalid'),('teacher','Teacher','teacher@example.invalid');
    INSERT INTO project_memberships VALUES('tenant','project','learner','ACTIVE','STUDENT'),('tenant','project','observer','ACTIVE','OBSERVER'),('tenant','project','inactive','INACTIVE','STUDENT'),('tenant','project','teacher','ACTIVE','TEACHER');
    CREATE TABLE evidence_records(id text PRIMARY KEY,company_id text,project_id text,data jsonb);
    ALTER TABLE learning_attempts ADD COLUMN evidence_id text, ADD COLUMN mission_step_id text, ADD COLUMN assistance text, ADD COLUMN status text, ADD COLUMN submitted_at timestamptz;
    ALTER TABLE learning_evaluations ADD COLUMN demonstrated_level int, ADD COLUMN confidence numeric, ADD COLUMN feedback text;
    INSERT INTO evidence_records VALUES('evidence','tenant','project','{"answer":"observed"}'),('foreign-evidence','other','project','{"answer":"foreign"}');
    UPDATE learning_attempts SET evidence_id='evidence' WHERE id='attempt';
    UPDATE learning_attempts SET evidence_id='foreign-evidence' WHERE id='foreign';
  `)
  assert.deepEqual(await repository.findTeacherLearner(db, scope, 'learner'), { display_name: 'Learner', email: 'learner@example.invalid' })
  assert.deepEqual(await repository.findTeacherLearner(db, scope, 'observer'), { display_name: 'Observer', email: 'observer@example.invalid' })
  for (const learner of ['inactive', 'teacher', 'missing']) assert.equal(await repository.findTeacherLearner(db, scope, learner), undefined)
  for (const other of [{ ...scope, companyId: 'other' }, { ...scope, projectId: 'other' }]) {
    assert.equal(await repository.findTeacherLearner(db, other, 'learner'), undefined)
    assert.equal(await repository.findTeacherAttemptDetail(db, other, 'attempt'), undefined)
  }
  const attempt = await repository.findTeacherAttemptDetail(db, scope, 'attempt')
  assert.deepEqual(attempt.evidence, { answer: 'observed' })
  assert.deepEqual(attempt.evaluations.map(row => row.id).sort(), ['accepted', 'pending'])
  assert.equal(attempt.learner_id, 'learner')
  assert.equal(await repository.findTeacherAttemptDetail(db, scope, 'foreign'), undefined)
  await db.exec("UPDATE learning_attempts SET evidence_id='foreign-evidence' WHERE id='attempt'")
  assert.equal(await repository.findTeacherAttemptDetail(db, scope, 'attempt'), undefined)
  await db.exec("UPDATE learning_attempts SET evidence_id='evidence' WHERE id='attempt'")
  await db.exec(`
    ALTER TABLE learning_knowledge_units ADD COLUMN title text;
    UPDATE learning_knowledge_units SET title=id;
    CREATE TABLE learning_states(company_id text,project_id text,user_id text,knowledge_unit_id text,level int,status text,next_review_at timestamptz,review_interval_days int);
    CREATE TABLE learning_missions(id text PRIMARY KEY,company_id text,project_id text,learner_id text,goal text,success_criteria text,status text,updated_at timestamptz);
    INSERT INTO learning_states VALUES('tenant','project','learner','unit',2,'ACTIVE',NULL,7),('tenant','project','learner','other-unit',4,'ACTIVE',NULL,7),('tenant','project','observer','unit',3,'ACTIVE',NULL,7),('tenant','other','learner','other-project',1,'ACTIVE',NULL,7);
    INSERT INTO learning_missions VALUES('mission','tenant','project','learner','Goal','Criteria','ACTIVE',NOW()),('other-learner','tenant','project','observer','Other','Other','ACTIVE',NOW()),('foreign','other','project','learner','Foreign','Foreign','ACTIVE',NOW());
    UPDATE learning_evaluations SET created_at='2026-01-01' WHERE id='pending';
    UPDATE learning_evaluations SET created_at='2026-01-02',feedback='Latest scoped feedback' WHERE id='accepted';
    UPDATE learning_evaluations SET created_at='2026-01-03',feedback='Foreign feedback' WHERE id='wrong-scope';
  `)
  await db.exec(`
    CREATE TABLE attention_items(id text PRIMARY KEY,company_id text,project_id text,learner_user_id text,teacher_user_id text,reason text,status text,deferred_until timestamptz);
    INSERT INTO attention_items VALUES
      ('open','tenant','project','learner','teacher-a','needs_help','OPEN',NULL),
      ('acknowledged','tenant','project','learner','teacher-b','follow_up','ACKNOWLEDGED',NULL),
      ('closed','tenant','project','observer','teacher-a','closed','RESOLVED',NULL),
      ('future','tenant','project','observer','teacher-a','future','DEFERRED',NOW()+INTERVAL '1 day'),
      ('foreign','other','project','observer','teacher-a','foreign','OPEN',NULL),
      ('foreign-project','tenant','other','observer','teacher-a','foreign-project','OPEN',NULL);
  `)
  const learners = await repository.listTeacherLearnerRows(db, scope, false)
  assert.deepEqual(learners.map(row => row.user_id).sort(), ['learner', 'observer'])
  assert.deepEqual((await repository.listTeacherLearnerRows(db, scope, true)).map(row => row.user_id), ['learner'])
  const teacherAttention = await repository.listTeacherLearnerRows(db, { ...scope, teacherUserId: 'teacher-a' }, true)
  assert.deepEqual(teacherAttention.map(({ user_id, attention_reasons }) => ({ user_id, attention_reasons })), [{ user_id: 'learner', attention_reasons: ['needs_help'] }])
  assert.deepEqual(await repository.listTeacherLearnerRows(db, { ...scope, teacherUserId: 'absent' }, true), [])
  await db.exec("UPDATE attention_items SET deferred_until=NOW()-INTERVAL '1 day' WHERE id='future'")
  assert.deepEqual((await repository.listTeacherLearnerRows(db, scope, true)).map(row => row.user_id).sort(), ['learner', 'observer'])
  assert.deepEqual(await repository.listTeacherLearnerRows(db, { ...scope, companyId: 'absent' }, false), [])
  assert.deepEqual(await repository.listTeacherLearnerRows(db, { ...scope, projectId: 'absent' }, false), [])
  const detail = await repository.loadTeacherLearnerDetailRows(db, scope, 'learner')
  assert.deepEqual(detail.states.map(({ knowledge_unit_id, level }) => ({ knowledge_unit_id, level })), [{ knowledge_unit_id: 'unit', level: 2 }])
  assert.deepEqual(detail.missions.map(row => row.id), ['mission'])
  assert.deepEqual(detail.attempts.map(({ id, evaluation_status, feedback }) => ({ id, evaluation_status, feedback })), [{ id: 'attempt', evaluation_status: 'ACCEPTED', feedback: 'Latest scoped feedback' }])
  assert.deepEqual(await repository.loadTeacherLearnerDetailRows(db, scope, 'missing'), { states: [], missions: [], attempts: [] })
  await db.exec("INSERT INTO learning_missions SELECT 'mission-'||i,'tenant','project','learner','Goal','Criteria','ACTIVE',NOW()+i*INTERVAL '1 minute' FROM generate_series(1,25) i")
  assert.equal((await repository.loadTeacherLearnerDetailRows(db, scope, 'learner')).missions.length, 20)
  await db.exec("INSERT INTO learning_evaluations(id,company_id,project_id,attempt_id,status,created_at) SELECT 'review-'||i,'tenant','project','attempt','PENDING',NOW()+i*INTERVAL '1 minute' FROM generate_series(1,105) i")
  assert.equal((await repository.listTeacherReviews(db, scope)).length, 100)
  await db.exec(`
    CREATE TABLE courses(id text PRIMARY KEY,company_id text,project_id text,study_room_conversation_id text);
    CREATE TABLE conversations(id text PRIMARY KEY,company_id text,project_id text,title text,kind text,updated_at timestamptz);
    CREATE TABLE learning_course_rooms(company_id text,conversation_id text,course_id text,purpose text);
    CREATE TABLE learning_course_teacher_rooms(company_id text,conversation_id text);
    INSERT INTO courses VALUES('course','tenant','project','study');
    INSERT INTO conversations VALUES
      ('study','tenant','project','Study','group',NOW()),
      ('bound','tenant','project','Bound','group',NOW()),
      ('free','tenant','project','Free','group',NOW()),
      ('teacher','tenant','project','Teacher','group',NOW()),
      ('other-course','tenant','project','Other course','group',NOW()),
      ('other-project','tenant','elsewhere','Other project','group',NOW()),
      ('foreign','other','project','Foreign','group',NOW()),
      ('direct','tenant','project','Direct','direct',NOW());
    INSERT INTO learning_course_rooms VALUES('tenant','bound','course','discussion'),('tenant','other-course','elsewhere','discussion');
    INSERT INTO learning_course_teacher_rooms VALUES('tenant','teacher');
  `)
  const rooms = await repository.listTeacherBindableRooms(db, scope)
  assert.deepEqual(rooms.sort((a, b) => a.conversation_id.localeCompare(b.conversation_id)), [
    { conversation_id: 'bound', title: 'Bound', purpose: 'discussion', bound: true },
    { conversation_id: 'free', title: 'Free', purpose: null, bound: null },
    { conversation_id: 'study', title: 'Study', purpose: 'study', bound: true },
  ])
  assert.deepEqual(await repository.listTeacherBindableRooms(db, { ...scope, companyId: 'other' }), [])
  assert.deepEqual(await repository.listTeacherBindableRooms(db, { ...scope, courseId: 'absent' }), [])
  await db.exec("INSERT INTO conversations SELECT 'room-'||i,'tenant','project','Room','group',NOW()+i*INTERVAL '1 minute' FROM generate_series(1,105) i")
  assert.equal((await repository.listTeacherBindableRooms(db, scope)).length, 100)
  await db.exec(`
    ALTER TABLE learning_knowledge_units ADD COLUMN status text;
    ALTER TABLE attention_items ADD COLUMN rank_score int, ADD COLUMN expected_minutes int;
    INSERT INTO learning_knowledge_units(id,company_id,project_id,position,title,status) VALUES('overview-unit','tenant','overview',1,'Unit','ACTIVE');
    INSERT INTO project_memberships VALUES('tenant','overview','learner','ACTIVE','STUDENT'),('tenant','overview','observer','ACTIVE','OBSERVER');
    INSERT INTO learning_states VALUES('tenant','overview','learner','overview-unit',3,'ACTIVE',NOW()-INTERVAL '1 day',7);
    INSERT INTO learning_attempts(id,company_id,project_id,learner_id,activity_id,submitted_at) VALUES('overview-attempt','tenant','overview','learner','overview-activity',NOW());
    INSERT INTO learning_evaluations(id,company_id,project_id,attempt_id,status,created_at) VALUES('overview-evaluation','tenant','overview','overview-attempt','ACCEPTED',NOW());
    INSERT INTO learning_missions VALUES('overview-mission','tenant','overview','learner','Goal','Criteria','ACTIVE',NOW());
    INSERT INTO attention_items(id,company_id,project_id,learner_user_id,teacher_user_id,reason,status,rank_score,expected_minutes) VALUES('overview-attention','tenant','overview','learner','teacher-a','needs_help','OPEN',3,5);
  `)
  const overview = await repository.loadTeacherOverviewRows(db, { ...scope, projectId: 'overview', teacherUserId: 'teacher-a' }, 7)
  assert.deepEqual(overview, {
    distribution: [1, 0, 0, 1, 0].map((knowledge_unit_states, level) => ({ level, knowledge_unit_states })),
    missions: [{ status: 'ACTIVE', count: 1 }],
    activity: [{ attempts: 1, pending_reviews: 0, accepted_evaluations: 1, rejected_evaluations: 0 }],
    attention: [{ user_id: 'learner', display_name: 'Learner', attention_reasons: ['needs_help'], rank_score: 3, expected_minutes: 5 }],
    coverage: [{ learners: 2, learners_with_evidence: 1, verified_attempts: 1, due_reviews: 1 }],
  })
  assert.deepEqual((await repository.loadTeacherOverviewRows(db, { ...scope, projectId: 'overview', teacherUserId: 'absent' }, 7)).attention, [])
  const managementInput = await readFile(resolve(source, 'modules/learning/teacher-management-repository.ts'), 'utf8')
  const managementOutput = ts.transpileModule(managementInput, { compilerOptions: { target: ts.ScriptTarget.ES2023, module: ts.ModuleKind.ESNext } }).outputText
  const management = await import(`data:text/javascript;base64,${Buffer.from(managementOutput).toString('base64')}`)
  await db.exec(`
    CREATE TABLE projects(id text PRIMARY KEY,company_id text,name text,description text,updated_at timestamptz);
    CREATE TABLE participants(id text PRIMARY KEY,company_id text,name text,updated_at timestamptz);
    CREATE TABLE learning_project_teacher_agents(company_id text,project_id text,agent_id text);
    INSERT INTO projects VALUES('project','tenant','Original','Description',NOW());
    INSERT INTO participants VALUES('teacher-agent','tenant','Pulse · Original',NOW());
    INSERT INTO learning_project_teacher_agents VALUES('tenant','project','teacher-agent');
  `)
  assert.equal(await management.updateTeacherCourseMetadata(db, { companyId: 'other', courseId: 'course', title: 'Foreign' }), undefined)
  await db.exec('BEGIN')
  try {
    const failing = { query: (sql, args) => {
      if (sql.includes('UPDATE participants')) throw new Error('injected participant failure')
      return db.query(sql, args)
    } }
    await assert.rejects(management.updateTeacherCourseMetadata(failing, { companyId: 'tenant', courseId: 'course', title: 'Incomplete' }), /injected participant failure/)
  } finally { await db.exec('ROLLBACK') }
  assert.deepEqual((await db.query('SELECT name FROM projects')).rows, [{ name: 'Original' }])
  assert.deepEqual((await db.query('SELECT name FROM participants')).rows, [{ name: 'Pulse · Original' }])
  await db.exec('BEGIN')
  try {
    const updated = await management.updateTeacherCourseMetadata(db, { companyId: 'tenant', courseId: 'course', title: 'Updated', description: 'New description' })
    assert.equal(updated.name, 'Updated')
    await db.exec('COMMIT')
  } catch (error) { await db.exec('ROLLBACK'); throw error }
  assert.deepEqual((await db.query('SELECT name,description FROM projects')).rows, [{ name: 'Updated', description: 'New description' }])
  assert.deepEqual((await db.query('SELECT name FROM participants')).rows, [{ name: 'Pulse · Updated' }])
  // Resolve local imports without changing the native repository implementation.
  const modules = {}
  for (const name of ['project-scope-repository', 'activities-repository', 'curriculum-repository']) {
    const input = await readFile(resolve(source, `modules/learning/${name}.ts`), 'utf8')
    let output = ts.transpileModule(input, { compilerOptions: { target: ts.ScriptTarget.ES2023, module: ts.ModuleKind.ESNext } }).outputText
    for (const [dependency, url] of Object.entries(modules)) output = output.replaceAll(`'./${dependency}.js'`, `'${url}'`)
    modules[name] = `data:text/javascript;base64,${Buffer.from(output).toString('base64')}`
  }
  const curriculum = await import(modules['curriculum-repository'])
  const queryable = { query: async (sql, params) => {
    const result = await db.query(sql, params)
    return { rows: result.rows, rowCount: result.affectedRows ?? result.rows.length }
  } }
  await db.exec(`
    ALTER TABLE projects ADD COLUMN kind text DEFAULT 'TEACHING', ADD COLUMN status text DEFAULT 'ACTIVE';
    ALTER TABLE learning_knowledge_units ADD COLUMN success_criteria text, ADD COLUMN target_level int, ADD COLUMN created_by text, ADD COLUMN created_at timestamptz DEFAULT NOW();
    ALTER TABLE learning_knowledge_unit_dependencies ADD UNIQUE(company_id,project_id,knowledge_unit_id,prerequisite_knowledge_unit_id);
  `)
  const draft = { id: 'new-draft', companyId: 'tenant', projectId: 'project', actorId: 'teacher', title: 'Draft', successCriteria: 'Explain', targetLevel: 3, position: 0 }
  await assert.rejects(curriculum.insertLearningKnowledgeUnit(queryable, { ...draft, companyId: 'other' }), /project not found/)
  for (const prerequisiteKnowledgeUnitId of ['other-unit', 'other-project', 'missing']) {
    await db.exec('BEGIN')
    try {
      await curriculum.insertLearningKnowledgeUnit(queryable, draft)
      await assert.rejects(curriculum.insertLearningKnowledgeUnitDependency(queryable, { ...draft, knowledgeUnitId: draft.id, prerequisiteKnowledgeUnitId }), /current project/)
    } finally { await db.exec('ROLLBACK') }
    assert.deepEqual((await db.query("SELECT id FROM learning_knowledge_units WHERE id='new-draft'")).rows, [])
  }
  await db.exec('BEGIN')
  try {
    await curriculum.insertLearningKnowledgeUnit(queryable, draft)
    await curriculum.insertLearningKnowledgeUnitDependency(queryable, { ...draft, knowledgeUnitId: draft.id, prerequisiteKnowledgeUnitId: 'unit' })
    await db.exec('COMMIT')
  } catch (error) { await db.exec('ROLLBACK'); throw error }
  const objectives = await curriculum.listLearningObjectives(queryable, 'tenant', 'course')
  assert.deepEqual(objectives.find(row => row.id === draft.id), { id: draft.id, courseId: 'course', title: 'Draft', successCriteria: 'Explain', targetLevel: 3, position: 0, status: 'DRAFT', prerequisiteIds: ['unit'] })
  await assert.rejects(curriculum.listLearningObjectives(queryable, 'other', 'course'), /course not found/)
  const activities = await import(modules['activities-repository'])
  await db.exec(`
    ALTER TABLE learning_activities ADD COLUMN title text, ADD COLUMN instructions text, ADD COLUMN kind text,
      ADD COLUMN evaluation_mode text, ADD COLUMN target_level int, ADD COLUMN rubric jsonb,
      ADD COLUMN due_at timestamptz, ADD COLUMN created_by text, ADD COLUMN status text DEFAULT 'DRAFT';
    CREATE TABLE learning_activity_knowledge_units(company_id text,project_id text,activity_id text,knowledge_unit_id text,
      UNIQUE(company_id,project_id,activity_id,knowledge_unit_id));
  `)
  const activity = { id: 'draft-activity', companyId: 'tenant', projectId: 'project', actorId: 'teacher', title: 'Practice', instructions: 'Explain fractions', kind: 'PRACTICE', evaluationMode: 'TEACHER_REQUIRED', targetLevel: 2, rubric: [{ criterion: 'Compare' }], knowledgeUnitIds: ['unit', 'new-draft'] }
  for (const patch of [{ companyId: 'other' }, { projectId: 'missing' }, { knowledgeUnitIds: ['unit', 'other-unit'] }, { knowledgeUnitIds: ['unit', 'other-project'] }, { knowledgeUnitIds: ['missing'] }]) {
    await assert.rejects(activities.insertProjectLearningActivity(queryable, { ...activity, ...patch }), /project or knowledge unit not found/)
    assert.deepEqual((await db.query("SELECT id FROM learning_activities WHERE id='draft-activity'")).rows, [])
    assert.deepEqual((await db.query('SELECT * FROM learning_activity_knowledge_units')).rows, [])
  }
  await activities.insertProjectLearningActivity(queryable, activity)
  assert.deepEqual(await activities.findLearningActivity(queryable, 'tenant', 'course', activity.id), {
    id: activity.id, courseId: 'course', title: 'Practice', instructions: 'Explain fractions', type: 'PRACTICE',
    status: 'DRAFT', evaluationMode: 'TEACHER_REQUIRED', targetLevel: 2, rubric: [{ criterion: 'Compare' }], objectiveIds: ['new-draft', 'unit'],
  })
  assert.equal(await activities.findProjectLearningActivity(queryable, 'other', 'project', activity.id), null)
  assert.equal(await activities.findProjectLearningActivity(queryable, 'tenant', 'other', activity.id), null)
  const roomModules = {}
  for (const name of ['role', 'permission', 'public', 'rooms-repository']) {
    const path = name === 'rooms-repository' ? 'modules/learning/rooms-repository.ts' : `domain/access/${name}.ts`
    const input = await readFile(resolve(source, path), 'utf8')
    let output = ts.transpileModule(input, { compilerOptions: { target: ts.ScriptTarget.ES2023, module: ts.ModuleKind.ESNext } }).outputText
    for (const [dependency, url] of Object.entries(roomModules)) output = output.replaceAll(`'./${dependency}.js'`, `'${url}'`).replaceAll(`'../../domain/access/${dependency}.js'`, `'${url}'`)
    roomModules[name] = `data:text/javascript;base64,${Buffer.from(output).toString('base64')}`
  }
  const roomRepository = await import(roomModules['rooms-repository'])
  await db.exec('ALTER TABLE learning_course_rooms ADD COLUMN created_by text, ADD UNIQUE(conversation_id)')
  const binding = { companyId: 'tenant', courseId: 'course', conversationId: 'free', purpose: 'lab', createdBy: 'teacher' }
  for (const patch of [{ companyId: 'other' }, { courseId: 'missing' }, { conversationId: 'other-project' }, { conversationId: 'foreign' }, { conversationId: 'direct' }, { conversationId: 'teacher' }]) {
    assert.equal(await roomRepository.upsertLearningCourseRoom(queryable, { ...binding, ...patch }), false)
  }
  assert.equal(await roomRepository.upsertLearningCourseRoom(queryable, binding), true)
  assert.deepEqual((await db.query("SELECT course_id,purpose,created_by FROM learning_course_rooms WHERE conversation_id='free'")).rows, [{ course_id: 'course', purpose: 'lab', created_by: 'teacher' }])
  assert.equal(await roomRepository.upsertLearningCourseRoom(queryable, { ...binding, purpose: 'discussion' }), true)
  for (const patch of [{ companyId: 'other' }, { courseId: 'other-course' }]) {
    assert.equal(await roomRepository.deleteLearningCourseRoom(queryable, { ...binding, ...patch }), false)
  }
  assert.deepEqual((await db.query("SELECT purpose FROM learning_course_rooms WHERE conversation_id='free'")).rows, [{ purpose: 'discussion' }])
  assert.equal(await roomRepository.deleteLearningCourseRoom(queryable, binding), true)
  assert.equal(await roomRepository.deleteLearningCourseRoom(queryable, binding), false)
  await db.exec(`
    ALTER TABLE courses ADD COLUMN created_by text DEFAULT 'creator';
    ALTER TABLE projects ADD COLUMN created_by text DEFAULT 'creator';
    ALTER TABLE project_memberships ADD COLUMN updated_at timestamptz, ALTER COLUMN status SET DEFAULT 'ACTIVE', ADD UNIQUE(user_id,project_id);
    CREATE TABLE company_memberships(company_id text,user_id text,status text);
    INSERT INTO company_memberships VALUES('tenant','new-learner','ACTIVE'),('tenant','teacher','ACTIVE'),('tenant','owner','ACTIVE'),('tenant','creator','ACTIVE'),('tenant','suspended','SUSPENDED'),('other','foreign-member','ACTIVE');
    INSERT INTO project_memberships(company_id,project_id,user_id,status,role) VALUES('tenant','project','owner','ACTIVE','OWNER'),('tenant','project','creator','ACTIVE','STUDENT');
  `)
  const member = { companyId: 'tenant', courseId: 'course', userId: 'new-learner', role: 'learner', enabled: true }
  for (const patch of [{ companyId: 'other' }, { courseId: 'missing' }, { userId: 'suspended' }, { userId: 'foreign-member' }, { userId: 'missing' }]) {
    assert.equal(await roomRepository.setLearningCourseMembershipRecord(queryable, { ...member, ...patch }), 'not_found')
  }
  assert.equal(await roomRepository.setLearningCourseMembershipRecord(queryable, member), 'updated')
  assert.deepEqual((await db.query("SELECT role,status FROM project_memberships WHERE user_id='new-learner'")).rows, [{ role: 'STUDENT', status: 'ACTIVE' }])
  for (const userId of ['teacher', 'owner']) {
    assert.equal(await roomRepository.setLearningCourseMembershipRecord(queryable, { ...member, userId }), 'updated')
    assert.equal(await roomRepository.setLearningCourseMembershipRecord(queryable, { ...member, userId, enabled: false }), 'updated')
  }
  assert.deepEqual((await db.query("SELECT user_id,role FROM project_memberships WHERE project_id='project' AND user_id IN ('teacher','owner') ORDER BY user_id")).rows, [{ user_id: 'owner', role: 'OWNER' }, { user_id: 'teacher', role: 'TEACHER' }])
  assert.equal(await roomRepository.setLearningCourseMembershipRecord(queryable, { ...member, userId: 'creator', enabled: false }), 'protected_creator')
  assert.equal(await roomRepository.setLearningCourseMembershipRecord(queryable, { ...member, enabled: false }), 'updated')
  assert.deepEqual((await db.query("SELECT user_id FROM project_memberships WHERE user_id='new-learner'")).rows, [])
  const approvalInput = await readFile(resolve(source, 'modules/learning/teacher-approval-repository.ts'), 'utf8')
  const approvalOutput = ts.transpileModule(approvalInput, { compilerOptions: { target: ts.ScriptTarget.ES2023, module: ts.ModuleKind.ESNext } }).outputText
  const approvalRepository = await import(`data:text/javascript;base64,${Buffer.from(approvalOutput).toString('base64')}`)
  await db.exec(`
    ALTER TABLE learning_knowledge_units ADD COLUMN updated_at timestamptz DEFAULT '2026-01-01T00:00:00Z';
    ALTER TABLE learning_activities ADD COLUMN updated_at timestamptz DEFAULT '2026-01-01T00:00:00Z';
    ALTER TABLE learning_course_teacher_rooms ADD COLUMN course_id text DEFAULT 'course';
  `)
  for (const [target, version, id, table] of [
    ['findTeacherObjectiveApprovalTarget', 'findTeacherObjectiveApprovalVersion', 'new-draft', 'learning_knowledge_units'],
    ['findTeacherActivityApprovalTarget', 'findTeacherActivityApprovalVersion', 'draft-activity', 'learning_activities'],
  ]) {
    const preview = await approvalRepository[target](queryable, 'tenant', 'course', id)
    assert.equal(preview.status, 'DRAFT')
    assert.equal(String(await approvalRepository[version](queryable, 'tenant', 'teacher', id)), String(preview.updatedAt))
    for (const [tenant, course] of [['other', 'course'], ['tenant', 'missing']]) {
      assert.equal(await approvalRepository[target](queryable, tenant, course, id), undefined)
    }
    assert.equal(await approvalRepository[version](queryable, 'tenant', 'free', id), undefined)
    assert.equal(await approvalRepository[version](queryable, 'other', 'teacher', id), undefined)
    await db.query(`UPDATE ${table} SET updated_at='2026-01-02T00:00:00Z' WHERE id=$1`, [id])
    assert.notEqual(String(await approvalRepository[version](queryable, 'tenant', 'teacher', id)), String(preview.updatedAt))
  }
  const stateInput = await readFile(resolve(source, 'modules/learning/learning-state-repository.ts'), 'utf8')
  const stateOutput = ts.transpileModule(stateInput, { compilerOptions: { target: ts.ScriptTarget.ES2023, module: ts.ModuleKind.ESNext } }).outputText
  const stateRepository = await import(`data:text/javascript;base64,${Buffer.from(stateOutput).toString('base64')}`)
  await db.exec(`
    CREATE TABLE learning_mission_steps(id text,company_id text,project_id text,knowledge_unit_id text);
    ALTER TABLE learning_evaluations ADD COLUMN review_reason text, ADD COLUMN reviewed_by text, ADD COLUMN reviewed_at timestamptz;
    INSERT INTO learning_attempts(id,company_id,project_id,learner_id,activity_id,assistance) VALUES('review-attempt','tenant','project','learner','draft-activity','NONE');
    INSERT INTO learning_evaluations(id,company_id,project_id,attempt_id,status,demonstrated_level,confidence) VALUES('review-target','tenant','project','review-attempt','PENDING',3,0.8);
  `)
  const review = { companyId: 'tenant', projectId: 'project', evaluationId: 'review-target', status: 'ACCEPTED', reason: 'Verified evidence', reviewerId: 'teacher' }
  for (const patch of [{ companyId: 'other' }, { projectId: 'other' }, { evaluationId: 'missing' }]) {
    assert.equal(await stateRepository.lockPendingLearningEvaluation(queryable, { ...review, ...patch }), null)
    assert.equal(await stateRepository.reviewLearningEvaluationRecord(queryable, { ...review, ...patch }), false)
  }
  await db.exec('BEGIN')
  try {
    assert.deepEqual(await stateRepository.lockPendingLearningEvaluation(queryable, review), {
      attemptId: 'review-attempt', demonstratedLevel: 3, confidence: 0.8, userId: 'learner', assistance: 'NONE', activityType: 'PRACTICE', targetLevel: 2, knowledgeUnitIds: ['new-draft', 'unit'],
    })
    assert.equal(await stateRepository.reviewLearningEvaluationRecord(queryable, review), true)
  } finally { await db.exec('ROLLBACK') }
  assert.equal(await approvalRepository.findTeacherEvaluationApprovalVersion(queryable, 'tenant', 'teacher', 'review-target'), 'PENDING')
  assert.equal(await stateRepository.reviewLearningEvaluationRecord(queryable, review), true)
  assert.equal(await stateRepository.lockPendingLearningEvaluation(queryable, review), null)
  assert.equal(await stateRepository.reviewLearningEvaluationRecord(queryable, { ...review, status: 'REJECTED', reason: 'Duplicate' }), false)
  assert.deepEqual((await db.query("SELECT status,review_reason,reviewed_by,reviewed_at IS NOT NULL AS dated FROM learning_evaluations WHERE id='review-target'")).rows, [{ status: 'ACCEPTED', review_reason: 'Verified evidence', reviewed_by: 'teacher', dated: true }])
  assert.equal(await approvalRepository.findTeacherEvaluationApprovalVersion(queryable, 'tenant', 'teacher', 'review-target'), 'ACCEPTED')
  const projectionInput = await readFile(resolve(source, 'modules/learning/learning-state.ts'), 'utf8')
  const projectionOutput = ts.transpileModule(projectionInput, { compilerOptions: { target: ts.ScriptTarget.ES2023, module: ts.ModuleKind.ESNext } }).outputText
  const { projectLearningState } = await import(`data:text/javascript;base64,${Buffer.from(projectionOutput).toString('base64')}`)
  const projection = { previousLevel: 2, previousIndependentEvidenceCount: 0, demonstratedLevel: 3, assistance: 'NONE', confidence: 0.8, activityType: 'PRACTICE', activityTargetLevel: 3, evaluatorKind: 'TEACHER', teacherConfirmed: true, evidenceDistinct: true }
  for (const [patch, expected] of [
    [{}, [true, false, 2, 1, false]],
    [{ previousIndependentEvidenceCount: 1 }, [true, false, 3, 2, false]],
    [{ previousIndependentEvidenceCount: 1, evidenceDistinct: false }, [true, false, 2, 1, false]],
    [{ assistance: 'GUIDED' }, [true, false, 2, 0, false]],
    [{ confidence: 0.6 }, [false, true, 2, 0, false]],
    [{ activityType: 'ASSESSMENT' }, [true, false, 3, 1, false]],
    [{ demonstratedLevel: 4, activityTargetLevel: 4 }, [false, true, 2, 0, false]],
    [{ demonstratedLevel: 4, activityTargetLevel: 4, activityType: 'ASSESSMENT' }, [true, false, 4, 1, false]],
    [{ previousLevel: 3, demonstratedLevel: 1 }, [true, false, 3, 0, true]],
  ]) {
    const decision = projectLearningState({ ...projection, ...patch })
    assert.deepEqual([decision.accepted, decision.pendingTeacher, decision.nextLevel, decision.nextIndependentEvidenceCount, decision.needsReview], expected)
  }
  await db.exec(`
    ALTER TABLE learning_states ADD COLUMN independent_evidence_count int DEFAULT 0, ADD COLUMN last_evidence_at timestamptz,
      ADD COLUMN version int DEFAULT 1, ADD COLUMN updated_at timestamptz, ADD UNIQUE(project_id,user_id,knowledge_unit_id);
  `)
  const state = { companyId: 'tenant', projectId: 'project', userId: 'learner', knowledgeUnitId: 'new-draft', level: 3, status: 'VERIFIED', independentEvidenceCount: 2, reviewIntervalDays: 7 }
  for (const patch of [{ companyId: 'other' }, { projectId: 'other' }, { userId: 'missing' }, { knowledgeUnitId: 'other-unit' }, { knowledgeUnitId: 'other-project' }]) {
    assert.equal(await stateRepository.upsertLearningState(queryable, { ...state, ...patch }), false)
  }
  await db.exec('BEGIN')
  try {
    assert.deepEqual(await stateRepository.lockLearningState(queryable, state), { level: 0, independentEvidenceCount: 0, reviewIntervalDays: 1 })
    assert.equal(await stateRepository.upsertLearningState(queryable, state), true)
    assert.deepEqual(await stateRepository.lockLearningState(queryable, state), { level: 3, independentEvidenceCount: 2, reviewIntervalDays: 7 })
  } finally { await db.exec('ROLLBACK') }
  assert.deepEqual((await db.query("SELECT level FROM learning_states WHERE knowledge_unit_id='new-draft'")).rows, [])
  assert.equal(await stateRepository.upsertLearningState(queryable, state), true)
  assert.equal(await stateRepository.upsertLearningState(queryable, { ...state, status: 'NEEDS_REVIEW', reviewIntervalDays: 1 }), true)
  assert.deepEqual((await db.query("SELECT level,status,independent_evidence_count,review_interval_days,version,last_evidence_at IS NOT NULL AS dated,next_review_at>last_evidence_at AS scheduled FROM learning_states WHERE knowledge_unit_id='new-draft'")).rows,
    [{ level: 3, status: 'NEEDS_REVIEW', independent_evidence_count: 2, review_interval_days: 1, version: 2, dated: true, scheduled: true }])
  assert.equal(await stateRepository.markLearningAttemptEvaluated(queryable, { companyId: 'other', projectId: 'project', attemptId: 'review-attempt' }), false)
  assert.equal(await stateRepository.markLearningAttemptEvaluated(queryable, { companyId: 'tenant', projectId: 'project', attemptId: 'review-attempt' }), true)
  assert.deepEqual((await db.query("SELECT status FROM learning_attempts WHERE id='review-attempt'")).rows, [{ status: 'EVALUATED' }])
  const effectsInput = await readFile(resolve(source, 'modules/learning/effects-repository.ts'), 'utf8')
  const effectsOutput = ts.transpileModule(effectsInput, { compilerOptions: { target: ts.ScriptTarget.ES2023, module: ts.ModuleKind.ESNext } }).outputText
  const effectsUrl = `data:text/javascript;base64,${Buffer.from(effectsOutput).toString('base64')}`
  const effects = await import(effectsUrl)
  const baseline = await readFile(resolve(source, 'db/migrations/0001_v1_baseline.sql'), 'utf8')
  const effectSchema = baseline.match(/^CREATE TABLE public\.learning_effects \([\s\S]*?^\);/m)?.[0]
  assert.ok(effectSchema, 'native initial learning_effects schema is required')
  await db.exec('ALTER TABLE courses ADD UNIQUE(id,company_id)')
  await db.exec(effectSchema)
  const sync = { companyId: 'tenant', courseId: 'course', kind: 'teacher_room.sync' }
  await db.exec('BEGIN')
  await effects.enqueueLearningEffect(queryable, sync)
  await db.exec('ROLLBACK')
  assert.deepEqual(await effects.claimLearningEffects(queryable), [])
  await effects.enqueueLearningEffect(queryable, { ...sync, payload: { revision: 1 } })
  await effects.enqueueLearningEffect(queryable, { ...sync, payload: { revision: 2 } })
  const [first] = await effects.claimLearningEffects(queryable)
  assert.ok(first)
  assert.deepEqual([first.generation, first.attempts, first.payload], [2, 1, { revision: 2 }])
  assert.deepEqual(await effects.claimLearningEffects(queryable), [])
  await effects.enqueueLearningEffect(queryable, { ...sync, payload: { revision: 3 } })
  await effects.completeLearningEffect(queryable, first)
  await assert.rejects(effects.completeLearningEffect(queryable, first), /lease lost/)
  assert.equal(await effects.renewLearningEffectLease(queryable, first), false)
  const [second] = await effects.claimLearningEffects(queryable)
  assert.deepEqual([second.generation, second.attempts, second.payload], [3, 1, { revision: 3 }])
  await effects.failLearningEffect(queryable, second, 'temporary channel failure')
  assert.deepEqual(await effects.claimLearningEffects(queryable), [])
  await db.exec("UPDATE learning_effects SET available_at=NOW()-INTERVAL '1 second'")
  const [retry] = await effects.claimLearningEffects(queryable)
  assert.equal(retry.attempts, 2)
  await db.exec("UPDATE learning_effects SET lease_expires_at=NOW()-INTERVAL '1 second'")
  const [replacement] = await effects.claimLearningEffects(queryable)
  assert.notEqual(replacement.leaseToken, retry.leaseToken)
  assert.equal(await effects.renewLearningEffectLease(queryable, retry), false)
  await assert.rejects(effects.completeLearningEffect(queryable, retry), /lease lost/)
  await effects.completeLearningEffect(queryable, replacement)
  assert.deepEqual((await db.query('SELECT status,payload,generation FROM learning_effects')).rows, [{ status: 'completed', payload: { revision: 3 }, generation: 3 }])
  const imInput = await readFile(resolve(source, 'im/wukong.ts'), 'utf8')
  const imOutput = ts.transpileModule(imInput, { compilerOptions: { target: ts.ScriptTarget.ES2023, module: ts.ModuleKind.ESNext } }).outputText
  const { WukongClient } = await import(`data:text/javascript;base64,${Buffer.from(imOutput).toString('base64')}`)
  const channelRequests = []
  const server = http.createServer(async (request, response) => {
    const chunks = []
    for await (const chunk of request) chunks.push(chunk)
    channelRequests.push({ method: request.method, path: request.url, body: JSON.parse(Buffer.concat(chunks).toString('utf8')) })
    response.writeHead(channelRequests.length === 1 ? 503 : 200, { 'content-type': 'application/json' })
    response.end('{}')
  })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  try {
    const client = new WukongClient({ apiUrl: `http://127.0.0.1:${server.address().port}`, wsUrl: '', apiToken: 'fixture', webhookSecret: 'fixture' })
    const profile = { channelId: 'teacher-room', channelType: 2, members: ['teacher', 'agent'] }
    await assert.rejects(client.upsertChannel(profile), /returned 503/)
    await client.upsertChannel(profile)
    assert.deepEqual(channelRequests, Array(2).fill({ method: 'POST', path: '/channel', body: { channel_id: 'teacher-room', channel_type: 2, large: 0, reset: 1, subscribers: ['teacher', 'agent'] } }))
  } finally {
    await new Promise(resolve => { server.close(resolve); server.closeAllConnections() })
  }
  const lifecycleInput = await readFile(resolve(source, 'domain/project/project.ts'), 'utf8')
  const lifecycleOutput = ts.transpileModule(lifecycleInput, { compilerOptions: { target: ts.ScriptTarget.ES2023, module: ts.ModuleKind.ESNext } }).outputText
  const { transitionProject } = await import(`data:text/javascript;base64,${Buffer.from(lifecycleOutput).toString('base64')}`)
  for (const [from, command, to] of [['ACTIVE', 'END', 'COURSE_ENDED'], ['COURSE_ENDED', 'ENTER_READ_ONLY', 'READ_ONLY'], ['READ_ONLY', 'ARCHIVE', 'ARCHIVED']]) {
    assert.deepEqual(transitionProject('TEACHING', from, command), { outcome: 'APPLIED', from, to })
    assert.deepEqual(transitionProject('TEACHING', to, command), { outcome: 'ALREADY_APPLIED', from: to, to })
  }
  assert.deepEqual(transitionProject('TEACHING', 'ACTIVE', 'ARCHIVE'), { outcome: 'INVALID', from: 'ACTIVE', to: null })
  const closureInput = await readFile(resolve(source, 'modules/learning/project-lifecycle-projection.ts'), 'utf8')
  const closureOutput = ts.transpileModule(closureInput, { compilerOptions: { target: ts.ScriptTarget.ES2023, module: ts.ModuleKind.ESNext } }).outputText
    .replace("from './effects-repository.js'", `from '${effectsUrl}'`)
  assert.ok(closureOutput.includes(effectsUrl), 'resolve the native effect import without changing projection logic')
  const { projectLifecycleProjection } = await import(`data:text/javascript;base64,${Buffer.from(closureOutput).toString('base64')}`)
  await db.exec("ALTER TABLE learning_course_teacher_rooms ADD COLUMN status text DEFAULT 'active', ADD COLUMN closed_at timestamptz; INSERT INTO learning_course_teacher_rooms(company_id,conversation_id,course_id) VALUES('other','foreign-teacher','course')")
  const closure = { companyId: 'tenant', projectId: 'project', status: 'READ_ONLY' }
  for (const input of [{ ...closure, companyId: 'absent' }, { ...closure, projectId: 'absent' }, { ...closure, status: 'COURSE_ENDED' }]) await projectLifecycleProjection(queryable, input)
  assert.deepEqual((await db.query("SELECT status FROM learning_course_teacher_rooms WHERE company_id='tenant'")).rows, [{ status: 'active' }])
  await db.exec('BEGIN')
  await projectLifecycleProjection(queryable, closure)
  assert.deepEqual((await db.query('SELECT company_id,status,closed_at IS NOT NULL AS dated FROM learning_course_teacher_rooms ORDER BY company_id')).rows,
    [{ company_id: 'other', status: 'active', dated: false }, { company_id: 'tenant', status: 'closed', dated: true }])
  assert.equal((await db.query("SELECT id FROM learning_effects WHERE kind='course_archive.sync'")).rows.length, 1)
  await db.exec('ROLLBACK')
  assert.deepEqual((await db.query("SELECT status FROM learning_course_teacher_rooms WHERE company_id='tenant'")).rows, [{ status: 'active' }])
  assert.deepEqual((await db.query("SELECT id FROM learning_effects WHERE kind='course_archive.sync'")).rows, [])
  await projectLifecycleProjection(queryable, closure)
  const closedAt = (await db.query("SELECT closed_at FROM learning_course_teacher_rooms WHERE company_id='tenant'")).rows[0].closed_at
  await projectLifecycleProjection(queryable, { ...closure, status: 'ARCHIVED' })
  assert.deepEqual((await db.query("SELECT closed_at FROM learning_course_teacher_rooms WHERE company_id='tenant'")).rows[0].closed_at, closedAt)
  assert.deepEqual((await db.query("SELECT company_id,course_id,payload,generation,status FROM learning_effects WHERE kind='course_archive.sync'")).rows,
    [{ company_id: 'tenant', course_id: 'course', payload: { projectId: 'project', archive: true, projectStatus: 'ARCHIVED' }, generation: 2, status: 'pending' }])
  console.log('Unmodified native teacher and learning-state SQL passed scoped reads/writes and rollback checks in PGlite; native state policy and course lifecycle projection passed. Full native schema, application authorization, archive effect delivery and the complete evaluation-to-state application flow remain unverified.')
} finally {
  await db.close()
}
