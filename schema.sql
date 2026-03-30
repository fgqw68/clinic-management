-- 1. Create Patients Registry
CREATE TABLE patients (
    patient_name TEXT NOT NULL,
    phone_number TEXT NOT NULL,
    patient_id TEXT, 
    last_visit_date DATE,
    notes TEXT, 
    PRIMARY KEY (patient_name, phone_number)
);

-- 2. Create Living Visit Records
CREATE TABLE visits (
    patient_name TEXT NOT NULL,
    phone_number TEXT NOT NULL,
    visit_date DATE NOT NULL DEFAULT CURRENT_DATE,
    is_pregnancy BOOLEAN DEFAULT FALSE,
    next_visit_planned_date DATE, 
    followup_status TEXT DEFAULT 'Pending' 
        CHECK (followup_status IN ('Pending', 'Visited', 'Discontinued', 'Unreachable')),
    remarks TEXT, 
    COLUMN gravida_status TEXT,
    PRIMARY KEY (patient_name, phone_number, visit_date),
    FOREIGN KEY (patient_name, phone_number) REFERENCES patients ON UPDATE CASCADE
);

-- 3. Pregnancy Registry (For EDC Tracking)
CREATE TABLE pregnancy_registry (
    patient_name TEXT,
    phone_number TEXT,
    gravida_status TEXT,
    edc_date DATE NOT NULL,
    status TEXT DEFAULT 'Active' CHECK (status IN ('Active', 'Dropped', 'Unreachable', 'Delivered')),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (patient_name, phone_number, gravida_status)
);

-- 4. Create Living Bookings
CREATE TABLE bookings (
    patient_name TEXT NOT NULL,
    phone_number TEXT NOT NULL,
    planned_date DATE NOT NULL,
    booked_by TEXT DEFAULT 'Auto',
    status TEXT DEFAULT '',
    PRIMARY KEY (patient_name, phone_number)
);

-- 5. Create Patient Follow-up Tasks (With Composite PK for Assignee)
CREATE TABLE patient_tasks (
    assignee TEXT NOT NULL,
    patient_name TEXT NOT NULL,
    phone_number TEXT NOT NULL,
    followup_type TEXT NOT NULL, -- e.g., '3-Day', '15-Day', 'Feedback'
    status TEXT DEFAULT 'Pending' CHECK (status IN ('Pending', 'Completed', 'Unreachable', 'Discontinued')),
    due_date DATE DEFAULT CURRENT_DATE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    created_by TEXT,
    PRIMARY KEY (assignee, patient_name, phone_number, followup_type),
    FOREIGN KEY (patient_name, phone_number) REFERENCES patients ON UPDATE CASCADE
);

-- 6. Create Admin Tasks (Doctor to Staff)
CREATE TABLE admin_tasks (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    assignee TEXT DEFAULT 'Nimisha',
    task_message TEXT NOT NULL,
    status TEXT DEFAULT 'Pending' CHECK (status IN ('Pending', 'Completed')),
    due_date DATE DEFAULT CURRENT_DATE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);


-- 7. Add staff config
CREATE TABLE staff_config (
    telegram_id BIGINT PRIMARY KEY,
    staff_name TEXT NOT NULL,
    role TEXT DEFAULT 'staff' CHECK (role IN ('admin', 'staff')),
    is_active BOOLEAN DEFAULT TRUE
);
-- 8. Add Indexes for High Performance
CREATE INDEX idx_pt_dashboard ON patient_tasks (assignee, status);
CREATE INDEX idx_at_dashboard ON admin_tasks (assignee, status);