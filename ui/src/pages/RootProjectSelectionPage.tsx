import React, { useContext, useEffect } from "react";
import {
  EuiCard,
  EuiFlexGrid,
  EuiFlexItem,
  EuiIcon,
  EuiSkeletonText,
  EuiPageTemplate,
  EuiText,
  EuiTitle,
  EuiHorizontalRule,
  EuiEmptyPrompt,
  EuiButton,
} from "@elastic/eui";
import { useLoadProjectsList } from "../contexts/ProjectListContext";
import { useNavigate, Link } from "react-router-dom";
import FeastIconBlue from "../graphics/FeastIconBlue";
import OperatorContext from "../contexts/OperatorContext";

const RootProjectSelectionPage = () => {
  const { isLoading, isSuccess, data } = useLoadProjectsList();
  const navigate = useNavigate();
  const operatorState = useContext(OperatorContext);

  useEffect(() => {
    if (data && data.default) {
      navigate(`/p/${data.default}`);
    }

    if (data && data.projects.length === 1) {
      navigate(`/p/${data.projects[0].id}`);
    }
  }, [data, navigate]);

  const projectCards = data?.projects.map((item, index) => {
    return (
      <EuiFlexItem key={index}>
        <EuiCard
          icon={<EuiIcon size="xxl" type={FeastIconBlue} />}
          title={`${item.name}`}
          description={item?.description || ""}
          onClick={() => {
            navigate(`/p/${item.id}`);
          }}
        />
      </EuiFlexItem>
    );
  });

  const hasProjects = isSuccess && data?.projects && data.projects.length > 0;

  return (
    <EuiPageTemplate panelled>
      <EuiPageTemplate.Section>
        <EuiTitle size="s">
          <h1>Welcome to Feast</h1>
        </EuiTitle>
        <EuiText>
          <p>
            {hasProjects
              ? "Select a project to explore its features, entities, and services."
              : "No projects available yet. Select a project from the dropdown once deployed."}
          </p>
        </EuiText>
        <EuiHorizontalRule margin="m" />
        {isLoading && <EuiSkeletonText lines={1} />}
        {hasProjects && (
          <EuiFlexGrid columns={3} gutterSize="l">
            {projectCards}
          </EuiFlexGrid>
        )}
        {isSuccess && !hasProjects && (
          <EuiEmptyPrompt
            iconType="database"
            title={<h3>No projects available</h3>}
            body={
              <p>
                {operatorState.enabled
                  ? "Projects appear here once FeatureStores are deployed and their registries are running."
                  : "Configure a project list to get started."}
              </p>
            }
            actions={
              operatorState.enabled
                ? [
                    <Link to="/discover" key="discover">
                      <EuiButton iconType="compass">
                        Discover Features
                      </EuiButton>
                    </Link>,
                    <Link to="/manage" key="manage">
                      <EuiButton iconType="managementApp">
                        Manage Feature Stores
                      </EuiButton>
                    </Link>,
                  ]
                : []
            }
          />
        )}
      </EuiPageTemplate.Section>
    </EuiPageTemplate>
  );
};

export default RootProjectSelectionPage;
