"""
============================================================
  PROGRESS TRACKER FOR STREAMLIT UI
============================================================
  Tracks and displays agent/sub-agent execution progress
============================================================
"""

import streamlit as st
from datetime import datetime
from typing import Dict, List, Optional
import threading


class ProgressTracker:
    """Tracks progress of agents and sub-agents (session-specific for multi-user support)"""
    
    # Class-level storage for all sessions
    _all_sessions = {}
    _global_lock = threading.Lock()
    
    def __init__(self, session_id: str):
        """Initialize tracker for a specific session
        
        Args:
            session_id: Unique session identifier (e.g., session_folder path)
        """
        self.session_id = session_id
        
        # Initialize session data if not exists
        with self.__class__._global_lock:
            if session_id not in self.__class__._all_sessions:
                self.__class__._all_sessions[session_id] = {
                    'agents': {},
                    'current_phase': None,
                    'start_time': None,
                    'end_time': None,
                    'lock': threading.Lock()
                }
    
    @property
    def progress_data(self):
        """Get progress data for this session (thread-safe)"""
        with self.__class__._global_lock:
            return self.__class__._all_sessions.get(self.session_id, {})
    
    @property
    def _lock(self):
        """Get lock for this session"""
        with self.__class__._global_lock:
            session_data = self.__class__._all_sessions.get(self.session_id, {})
            return session_data.get('lock', threading.Lock())
    
    def reset(self):
        """Reset progress data for this session"""
        with self._lock:
            with self.__class__._global_lock:
                self.__class__._all_sessions[self.session_id] = {
                    'agents': {},
                    'current_phase': None,
                    'start_time': None,
                    'end_time': None,
                    'lock': threading.Lock()
                }
    
    def start_phase(self, phase_name: str):
        """Start a new phase (e.g., 'P&L Analysis', 'BS Analysis')"""
        with self._lock:
            data = self.progress_data
            data['current_phase'] = phase_name
            data['start_time'] = datetime.now()
            
            if phase_name not in data['agents']:
                data['agents'][phase_name] = {
                    'status': 'running',
                    'sub_agents': {},
                    'start_time': datetime.now(),
                    'end_time': None
                }
    
    def update_sub_agent(self, phase_name: str, sub_agent_name: str, status: str):
        """
        Update sub-agent status
        
        Args:
            phase_name: e.g., 'P&L Analysis'
            sub_agent_name: e.g., 'pnl_analyst_1', 'pnl_evaluator_1'
            status: 'running', 'completed', 'failed'
        """
        with self._lock:
            data = self.progress_data
            if phase_name not in data['agents']:
                self.start_phase(phase_name)
            
            data['agents'][phase_name]['sub_agents'][sub_agent_name] = {
                'status': status,
                'timestamp': datetime.now()
            }
    
    def complete_phase(self, phase_name: str, status: str = 'completed'):
        """Mark a phase as completed"""
        with self._lock:
            data = self.progress_data
            if phase_name in data['agents']:
                data['agents'][phase_name]['status'] = status
                data['agents'][phase_name]['end_time'] = datetime.now()
    
    def get_phase_progress(self, phase_name: str) -> Dict:
        """Get progress for a specific phase"""
        with self._lock:
            data = self.progress_data
            if phase_name not in data['agents']:
                return {'total': 0, 'completed': 0, 'running': 0, 'failed': 0}
            
            sub_agents = data['agents'][phase_name]['sub_agents']
            
            total = len(sub_agents)
            completed = sum(1 for sa in sub_agents.values() if sa['status'] == 'completed')
            running = sum(1 for sa in sub_agents.values() if sa['status'] == 'running')
            failed = sum(1 for sa in sub_agents.values() if sa['status'] == 'failed')
            
            return {
                'total': total,
                'completed': completed,
                'running': running,
                'failed': failed,
                'progress': (completed / total * 100) if total > 0 else 0
            }
    
    def render_progress_ui(self):
        """Render the progress UI in Streamlit"""
        with self._lock:
            data = self.progress_data
            if not data.get('agents'):
                return
            
            # Make a copy to avoid holding the lock during rendering
            agents_copy = dict(data['agents'])
        
        st.markdown("### 📊 Agent Execution Progress")
        
        for phase_name, phase_data in agents_copy.items():
            with st.expander(f"**{phase_name}**", expanded=True):
                progress = self.get_phase_progress(phase_name)
                
                # Phase status
                status_emoji = {
                    'running': '🔄',
                    'completed': '✅',
                    'failed': '❌'
                }
                
                col1, col2, col3 = st.columns([2, 1, 1])
                
                with col1:
                    st.markdown(f"{status_emoji.get(phase_data['status'], '⏳')} **Status:** {phase_data['status'].upper()}")
                
                with col2:
                    if progress['total'] > 0:
                        st.metric("Progress", f"{progress['completed']}/{progress['total']}")
                
                with col3:
                    if phase_data['start_time']:
                        duration = (phase_data['end_time'] or datetime.now()) - phase_data['start_time']
                        st.metric("Duration", f"{duration.seconds}s")
                
                # Progress bar
                if progress['total'] > 0:
                    st.progress(progress['progress'] / 100, 
                               text=f"{progress['completed']} of {progress['total']} sub-agents completed")
                
                # Sub-agent breakdown
                if phase_data['sub_agents']:
                    st.markdown("**Sub-Agents:**")
                    
                    # Group by type (analysts, evaluators, senior)
                    analysts = {k: v for k, v in phase_data['sub_agents'].items() if 'analyst_' in k and 'evaluator' not in k}
                    evaluators = {k: v for k, v in phase_data['sub_agents'].items() if 'evaluator' in k}
                    senior = {k: v for k, v in phase_data['sub_agents'].items() if 'senior' in k or ('analyst' in k and 'evaluator' not in k and '_' not in k.split('_')[-1])}
                    
                    # Analysts
                    if analysts:
                        analyst_completed = sum(1 for sa in analysts.values() if sa['status'] == 'completed')
                        analyst_total = len(analysts)
                        st.markdown(f"- **Analysts:** {analyst_completed}/{analyst_total} completed")
                        
                        # Show individual analysts in columns
                        cols = st.columns(min(4, analyst_total))
                        for idx, (name, data) in enumerate(sorted(analysts.items())):
                            with cols[idx % len(cols)]:
                                status_icon = '✅' if data['status'] == 'completed' else '🔄' if data['status'] == 'running' else '❌'
                                st.caption(f"{status_icon} {name.split('_')[-1]}")
                    
                    # Evaluators
                    if evaluators:
                        eval_completed = sum(1 for sa in evaluators.values() if sa['status'] == 'completed')
                        eval_total = len(evaluators)
                        st.markdown(f"- **Evaluators:** {eval_completed}/{eval_total} completed")
                        
                        # Show individual evaluators in columns
                        cols = st.columns(min(4, eval_total))
                        for idx, (name, data) in enumerate(sorted(evaluators.items())):
                            with cols[idx % len(cols)]:
                                status_icon = '✅' if data['status'] == 'completed' else '🔄' if data['status'] == 'running' else '❌'
                                st.caption(f"{status_icon} {name.split('_')[-1]}")
                    
                    # Senior analyst
                    if senior:
                        for name, data in senior.items():
                            status_icon = '✅' if data['status'] == 'completed' else '🔄' if data['status'] == 'running' else '❌'
                            st.markdown(f"- **Senior Analyst:** {status_icon} {data['status']}")
                
                st.divider()


# Session-specific tracker instances
_trackers = {}
_trackers_lock = threading.Lock()

def get_tracker(session_id: str = None) -> ProgressTracker:
    """Get the progress tracker instance for a specific session
    
    Args:
        session_id: Unique session identifier (e.g., session_folder path)
                   If None, uses a default tracker for backward compatibility
    
    Returns:
        ProgressTracker instance for the session
    """
    if session_id is None:
        session_id = 'default'
    
    with _trackers_lock:
        if session_id not in _trackers:
            _trackers[session_id] = ProgressTracker(session_id)
        return _trackers[session_id]

def cleanup_old_trackers(max_age_hours: int = 24):
    """Clean up tracker data for old sessions (memory optimization)
    
    Args:
        max_age_hours: Maximum age of session data to keep
    """
    from datetime import timedelta
    
    with _trackers_lock:
        current_time = datetime.now()
        sessions_to_remove = []
        
        for session_id, tracker in _trackers.items():
            data = tracker.progress_data
            start_time = data.get('start_time')
            
            if start_time and (current_time - start_time) > timedelta(hours=max_age_hours):
                sessions_to_remove.append(session_id)
        
        for session_id in sessions_to_remove:
            del _trackers[session_id]
            if session_id in ProgressTracker._all_sessions:
                del ProgressTracker._all_sessions[session_id]
