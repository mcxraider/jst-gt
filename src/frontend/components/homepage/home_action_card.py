# frontend/components/home_action_card.py
import streamlit as st


def home_action_card(pkl_available, load_checkpoint_enabled):
    st.markdown(
        """
    <div class="css-card">
        <h3 style="margin-top: 0;">Choose an Action:</h3>
    """,
        unsafe_allow_html=True,
    )

    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<div class="primary-button">', unsafe_allow_html=True)
        if st.button(
            f"⬆️ Upload New File & Run New Process",
            key="upload_new",
            use_container_width=True,
        ):
            st.session_state.app_stage = "uploading_new"
            st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)
    with col2:
        st.markdown('<div class="secondary-button">', unsafe_allow_html=True)
        if st.button(
            "🔄 Load from Previous Checkpoint",
            key="load_checkpoint",
            disabled=not load_checkpoint_enabled,
            use_container_width=True,
        ):
            st.session_state.app_stage = "load_checkpoint"
            st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)
