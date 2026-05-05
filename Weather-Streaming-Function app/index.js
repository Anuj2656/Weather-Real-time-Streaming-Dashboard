const { app } = require('@azure/functions');

app.setup({
    enableHttpStream: true,
});

// Import your function so it gets registered
require('./functions/function_app.js');
