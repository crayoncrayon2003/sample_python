// src/components/ParamsSidebar.jsx
import React from 'react';
import { useFlowStore } from '../store/useFlowStore';

// --- RJSF (React JSON Schema Form) Imports ---
import Form from '@rjsf/mui'; // Material-UI theme for the form
import validator from '@rjsf/validator-ajv8'; // The validator is required

const ParamsSidebar = () => {
  // Get the currently selected node's ID and the full list of nodes from the store
  const { nodes, selectedNodeId, updateNodeParams } = useFlowStore();
  
  // Find the complete node object based on the selected ID
  const selectedNode = nodes.find(node => node.id === selectedNodeId);

  // This function is called whenever the form data changes
  const handleParamsChange = ({ formData }) => {
    if (selectedNode) {
      // Update the node's parameters in our Zustand store
      updateNodeParams(selectedNodeId, formData);
    }
  };

  // If no node is selected, show a placeholder message
  if (!selectedNode) {
    return (
      <aside style={{
        borderLeft: '1px solid #eee',
        padding: '15px',
        width: '350px',
        backgroundColor: '#fafafa',
        overflowY: 'auto'
      }}>
        <h3 style={{ marginTop: 0 }}>Parameters</h3>
        <p style={{ color: '#777', fontSize: '14px' }}>
          Select a node on the canvas to view and edit its parameters here.
        </p>
      </aside>
    );
  }

  // If a node is selected, render the form
  const schema = selectedNode.data.pluginInfo.parameters_schema;
  const formData = selectedNode.data.params || {};

  return (
    <aside style={{
      borderLeft: '1px solid #eee',
      padding: '15px',
      width: '350px',
      overflowY: 'auto'
    }}>
      <h3 style={{ marginTop: 0 }}>Parameters for "{selectedNode.data.label}"</h3>
      
      {/* The Form component from RJSF does all the magic */}
      <Form
        schema={schema}
        formData={formData}
        validator={validator}
        onChange={handleParamsChange}
        // The form is updated "live" as you type, so no "Apply" button is needed
        liveValidate
        showErrorList={false}
      >
        {/* We can hide the default submit button as changes are applied live */}
        <button type="submit" style={{ display: 'none' }} />
      </Form>
    </aside>
  );
};

export default ParamsSidebar;