// Create Customer nodes (like a table with attributes)
CREATE (:Customer {
    id: 1, 
    name: 'Alice Johnson', 
    email: 'alice@example.com',
    address: '123 Main St',
    phone: '555-0101',
    registeredDate: date('2025-01-15')
});

CREATE (:Customer {
    id: 2, 
    name: 'Bob Smith', 
    email: 'bob@example.com',
    address: '456 Oak Ave',
    phone: '555-0102',
    registeredDate: date('2025-02-20')
});

CREATE (:Customer {
    id: 3, 
    name: 'Charlie Brown', 
    email: 'charlie@example.com',
    address: '789 Pine Rd',
    phone: '555-0103',
    registeredDate: date('2025-03-10')
});

// Create indexes for better query performance
CREATE INDEX customer_id_index IF NOT EXISTS FOR (c:Customer) ON (c.id);

// Create constraints for data integrity
CREATE CONSTRAINT customer_id_key IF NOT EXISTS FOR (c:Customer) REQUIRE c.id IS NODE KEY;
CREATE CONSTRAINT customer_email_unique IF NOT EXISTS FOR (c:Customer) REQUIRE c.email IS UNIQUE;